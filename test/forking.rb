
require 'rubygems'
require 'cassandra/0.7'
require 'lib/cassandra_lock'
require 'fileutils'


WORKER_COUNT = 10
#WORKER_COUNT = 1

#cl = Cassandra.new('CassandraLock')

CassandraLock.reset_lock('work', WORKER_COUNT)

#
# fork workers

worker_pids = []

WORKER_COUNT.times do |i|

  worker_pids << Process.fork do

    handle = CassandraLock::Handle.new(
      'work', i + 1, "127.0.0.#{ (i % 5) + 1 }:9160")

    #p handle.client

    loop do

      handle.lock

      #STDERR.puts "#{i} vvv"
      #sleep(0.1 * rand)
      #STDERR.puts "#{i} ^^^"

      handle.unlock
    end
  end
end

sleep 3 * 60

at_exit do
  worker_pids.each { |pid| Process.kill('INT', pid) }
end

