require 'rubygems'
require 'spec/rake/spectask'
require 'lib/cassandra_lock'

task :default => :spec

Spec::Rake::SpecTask.new(:spec) do |t|
  t.spec_opts = ["--colour --format=nested --backtrace"]
  t.spec_files = Dir['spec/*.rb'].sort
end

task :reset_keyspace do
  if Cassandra.VERSION < "0.7"
    raise "Need Cassandra 0.7 to manipulate keyspaces"
  end

  keyspace = ENV['KEYSPACE'] || "CassandraLock"
  host     = ENV['HOST']     || "127.0.0.1:9160"
  rf       = ENV['RF']       || "1"

  puts "Using values:"
  puts "KEYSPACE: #{keyspace}"
  puts "HOST: #{host}"
  puts "RF: #{rf}"
  puts "Dropping and re-adding keyspace..."

  c = Cassandra.new("system", host, :timeout => 5)
  c.drop_keyspace(keyspace) rescue nil

  CassandraLock.keyspace = keyspace

  ks_def = Cassandra::Keyspace.new(:name => keyspace,
                                   :strategy_class => "org.apache.cassandra.locator.RackUnawareStrategy",
                                   :replication_factor => rf.to_i,
                                   :cf_defs => CassandraLock.cf_defs)

  c.add_keyspace(ks_def)

  puts "Done"
end

task :reset_cfs do
  keyspace = ENV['KEYSPACE'] || "CassandraLock"
  host     = ENV['HOST']     || "127.0.0.1:9160"

  puts "Using values:"
  puts "KEYSPACE: #{keyspace}"
  puts "HOST: #{host}"
  puts "Resetting lock CFs..."

  CassandraLock.keyspace = keyspace
  CassandraLock.host = host
  CassandraLock.reset_cfs!

  puts "Done"
end
