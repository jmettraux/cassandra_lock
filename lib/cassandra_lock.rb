begin
  require 'cassandra/0.7'
rescue LoadError
  require 'cassandra'
end

class CassandraLock
  TRUE = "1"
  FALSE = "0"
  CHOOSING = "_locks_choosing"
  NUMBERS = "_locks_numbers"
  SLEEP_DURATION = 0.1

  class << self
    attr_writer :keyspace, :host

    def host
      @host || "127.0.0.1:9160"
    end

    def keyspace
      @keyspace || "CassandraLock"
    end

    def reset_cfs!
      if Cassandra.VERSION < "0.7"
        raise "Need Cassandra 0.7 to manipulate CFs"
      end

      client.drop_column_family(CHOOSING) rescue nil
      client.drop_column_family(NUMBERS) rescue nil

      cf_defs.each do |cf|
        client.add_column_family(cf)
      end

      nil
    end

    def setup_lock(lock_id, max_workers)
      if get(CHOOSING, lock_id).any? || get(NUMBERS, lock_id).any?
        raise "Lock #{lock_id} exists! use reset_lock()"
      end

      workers = (1..max_workers)
      empty = workers.inject({}){|acc, worker| acc.merge(worker.to_s => "0") }

      set(CHOOSING, lock_id, empty)
      set(NUMBERS,  lock_id, empty)

      nil
    end

    def reset_lock(lock_id, max_workers)
      delete_lock(lock_id)
      setup_lock(lock_id, max_workers)
    end

    def delete_lock(lock_id)
      client.remove(CHOOSING, lock_id, :consistency => Cassandra::Consistency::QUORUM)
      client.remove(NUMBERS,  lock_id, :consistency => Cassandra::Consistency::QUORUM)
    end

    def cf_defs
      [Cassandra::ColumnFamily.new(:keyspace => self.keyspace,
                                   :name => CHOOSING),
       Cassandra::ColumnFamily.new(:keyspace => self.keyspace,
                                   :name => NUMBERS)]
    end

    def get(cf, key)
      client.get(cf, key, :consistency => Cassandra::Consistency::QUORUM)
    end

    def set(cf, key, value)
      client.insert(cf, key, value, :consistency => Cassandra::Consistency::QUORUM)
    end

    def client
      @client ||= Cassandra.new(keyspace, host, :timeout => 5)
    end
  end

  def initialize(lock_id, my_worker_id, keyspace=nil)
    @keyspace = keyspace || self.class.keyspace
    @lock_id = lock_id
    @my_worker_id = my_worker_id
    @client = Cassandra.new(@keyspace)

    numbers = get(NUMBERS, @lock_id)

    if numbers.empty?
      raise "Lock '#{lock_id}' does not exist"
    end

    workers = numbers.keys.map{|n| n.to_i }
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
    worker_count, my_number = get_worker_count_and_current_number(@lock_id, @my_worker_id)
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
    choosing = get(CHOOSING, lock_id)
    numbers  = get(NUMBERS, lock_id)

    (1..worker_count).each do |worker_id|
      while is_worker_choosing?(choosing, worker_id)
        if should_wait
          sleep SLEEP_DURATION
          choosing = get(CHOOSING, lock_id)
        else
          return false
        end
      end

      while am_i_waiting_behind_worker?(numbers, worker_id, my_number, my_worker_id)
        if should_wait
          sleep SLEEP_DURATION
          numbers = get(NUMBERS, lock_id)
        else
          return false
        end
      end
    end

    true
  end

  def get_worker_count_and_current_number(lock_id, worker_id)
    worker_id = worker_id.to_s

    # indicate that we are in the process of picking a number
    set(CHOOSING, lock_id, { worker_id => TRUE })

    # get the current highest number, add 1, insert it as our number
    numbers = get(NUMBERS, lock_id)
    my_number = numbers.values.map{|n| n.to_i }.max + 1
    set(NUMBERS, lock_id, { worker_id => my_number.to_s })

    # indicate that we are done choosing our number
    set(CHOOSING, lock_id, { worker_id => FALSE })

    # return the total count of workers and our current number
    [numbers.size, my_number]
  end

  def get_out_of_line
    set(NUMBERS, @lock_id, { @my_worker_id.to_s => "0" })
  end

  def is_worker_choosing?(choosing, worker_id)
    choosing[worker_id.to_s] == TRUE
  end

  def am_i_waiting_behind_worker?(numbers, his_worker_id, my_number, my_worker_id)
    his_number = numbers[his_worker_id.to_s].to_i
    his_number != 0 && (his_number < my_number || (his_number == my_number && his_worker_id < my_worker_id))
  end

  def get(cf, key)
    @client.get(cf, key, :consistency => Cassandra::Consistency::QUORUM)
  end

  def set(cf, key, value)
    @client.insert(cf, key, value, :consistency => Cassandra::Consistency::QUORUM)
  end

end