
require 'rubygems'
require 'cassandra/0.7'
require 'lib/cassandra_lock'
require 'fileutils'


WORKER_COUNT = 10

#cl = Cassandra.new('CassandraLock')

CassandraLock.reset_lock('work', WORKER_COUNT)

#
# fork workers

worker_pids = []

WORKER_COUNT.times do |i|

  worker_pids << Process.fork do

    start = Time.now
    locks = 0

    handle = CassandraLock::Handle.new(
      'work', i + 1, "127.0.0.#{ (i % 5) + 1 }:9160")

    Signal.trap('INT') do
      duration = Time.now - start
      puts "#{i} : #{duration}s"
      puts "#{i} : #{locks} locks"
      puts "#{i} : #{locks/duration} locks/s"
      puts "#{i} : #{handle.gcounts} reads"
      puts "#{i} : #{handle.scounts} writes"
      exit(0)
    end

    loop do

      handle.lock

      #STDERR.puts "#{i} vvv"
      #sleep(0.1 * rand)
      #STDERR.puts "#{i} ^^^"

      handle.unlock
      locks += 1
    end
  end
end

sleep 60

at_exit do
  worker_pids.each { |pid| Process.kill('INT', pid) }
end

