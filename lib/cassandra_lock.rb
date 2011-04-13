begin
  require 'cassandra/0.7'
rescue LoadError
  require 'cassandra'
end

class CassandraLock
  TRUE = "1"
  FALSE = "0"
  CF = "_cassandra_locks"
  CHOOSING = "_choosing"
  NUMBERS = "_numbers"
  SLEEP_DURATION = 0.1

  class << self
    attr_writer :keyspace, :host

    def host
      @host || "127.0.0.1:9160"
    end

    def keyspace
      @keyspace || "CassandraLock"
    end

    def reset_cf!
      if Cassandra.VERSION < "0.7"
        raise "Need Cassandra 0.7 to manipulate CFs"
      end

      client.drop_column_family(CF) rescue nil

      sleep 1 # give some time to the schema to agree

      client.add_column_family(cf_def)

      sleep 1 # give some time to the schema to agree

      nil
    end

    def setup_lock(lock_id, max_workers)
      if get(lock_id)[NUMBERS]
        raise "Lock #{lock_id} exists! use reset_lock()"
      end

      workers = (1..max_workers)
      empty = workers.inject({}){|acc, worker| acc.merge(worker.to_s => "0") }

      set(lock_id, { CHOOSING => empty, NUMBERS => empty })
      nil
    end

    def reset_lock(lock_id, max_workers)
      delete_lock(lock_id)
      setup_lock(lock_id, max_workers)
    end

    def delete_lock(lock_id)
      client.remove(CF, lock_id, :consistency => Cassandra::Consistency::QUORUM)
    end

    def cf_def
      Cassandra::ColumnFamily.new(:keyspace => self.keyspace,
                                  :name => CF,
                                  :column_type => "Super")
    end

    def get(key)
      client.get(CF, key, :consistency => Cassandra::Consistency::QUORUM)
    end

    def set(key, value)
      client.insert(CF, key, value, :consistency => Cassandra::Consistency::QUORUM)
    end

    def client
      @client ||= Cassandra.new(keyspace, host, :timeout => 5)
    end
  end

  class Handle

    attr_reader :client

    def initialize(lock_id, my_worker_id, host, keyspace=nil)
      @keyspace = keyspace || CassandraLock.keyspace
      @lock_id = lock_id
      @my_worker_id = my_worker_id
      @client = Cassandra.new(@keyspace, host)
      @client.disable_node_auto_discovery!

      lock_data = get(@lock_id)

      unless lock_data[NUMBERS]
        raise "Lock '#{lock_id}' does not exist"
      end

      workers = lock_data[NUMBERS].keys.map{|n| n.to_i }
      range = (workers.min)..(workers.max)

      unless range.include?(my_worker_id)
        raise ArgumentError, "Worker ID #{my_worker_id} out of range. Valid range for this lock: #{range}"
      end
    end

    def lock
      worker_count, my_number = get_worker_count_and_current_number(@lock_id, @my_worker_id)
      acquire(@lock_id, worker_count, my_number, @my_worker_id, true)
    end

    def try_lock
      worker_count, my_number = get_worker_count_and_current_number(@lock_id, @my_worker_id, true)

      # bail early if someone was already holding/waiting
      unless my_number
        return false
      end

      lock_acquired = acquire(@lock_id, worker_count, my_number, @my_worker_id, false)

      unless lock_acquired
        get_out_of_line
      end

      lock_acquired
    end

    def unlock
      get_out_of_line
      nil
    end

    def synchronize
      unless block_given?
        raise ArgumentError, "Block required"
      end

      lock()
      yield
    ensure
      unlock()
    end

    private

    def acquire(lock_id, worker_count, my_number, my_worker_id, should_wait)
      lock_data = get(lock_id)

      (1..worker_count).each do |worker_id|
        while is_worker_choosing?(lock_data[CHOOSING], worker_id)
          if should_wait
            sleep SLEEP_DURATION
            lock_data = get(lock_id)
          else
            return false
          end
        end

        while am_i_waiting_behind_worker?(lock_data[NUMBERS], worker_id, my_number, my_worker_id)
          if should_wait
            sleep SLEEP_DURATION
            lock_data = get(lock_id)
          else
            return false
          end
        end
      end

      true
    end

    def get_worker_count_and_current_number(lock_id, worker_id, fail_fast=false)
      worker_id = worker_id.to_s

      # indicate that we are in the process of picking a number
      set(lock_id, { CHOOSING => { worker_id => TRUE } })

      # get the current highest number, add 1, insert it as our number
      numbers = get(lock_id)[NUMBERS]

      # for try_lock, just bail if anyone is holding/waiting
      if fail_fast
        if numbers.values.any?{|v| v != "0" }
          set(lock_id, { CHOOSING => { worker_id => FALSE } })
          return false
        end
      end

      my_number = numbers.values.map{|n| n.to_i }.max + 1
      set(lock_id, { NUMBERS => { worker_id => my_number.to_s } })

      # indicate that we are done choosing our number
      set(lock_id, { CHOOSING => { worker_id => FALSE } })

      # return the total count of workers and our current number
      [numbers.size, my_number]
    end

    def get_out_of_line
      set(@lock_id, { NUMBERS => { @my_worker_id.to_s => "0" } })
    end

    def is_worker_choosing?(choosing, worker_id)
      choosing[worker_id.to_s] == TRUE
    end

    def am_i_waiting_behind_worker?(numbers, his_worker_id, my_number, my_worker_id)
      his_number = numbers[his_worker_id.to_s].to_i
      his_number != 0 && (his_number < my_number || (his_number == my_number && his_worker_id < my_worker_id))
    end

    def get(key)
      @client.get(CF, key, :consistency => Cassandra::Consistency::QUORUM)
    end

    def set(key, value)
      @client.insert(CF, key, value, :consistency => Cassandra::Consistency::QUORUM)
    end

  end

end
