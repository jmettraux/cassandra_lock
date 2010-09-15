require 'lib/cassandra_lock'

describe "CassandraLock" do
  before(:all) do
    CassandraLock.reset_cf!
  end

  before(:each) do
    CassandraLock.reset_lock("test_lock", 10)
    @lock1 = CassandraLock.new("test_lock", 1)
    @lock2 = CassandraLock.new("test_lock", 2)
    @scratchpad = []
  end

  context "CassandraLock.new" do
    it "raises a RuntimeError when given an uninitialized lock id" do
      lambda { CassandraLock.new("non_existent_lock", 1) }.should raise_error(RuntimeError)
    end

    it "raises an ArgumentError when given a worker ID that is out of range of the lock" do
      lambda { CassandraLock.new("test_lock", 0) }.should raise_error(ArgumentError)
      lambda { CassandraLock.new("test_lock", 11) }.should raise_error(ArgumentError)
    end
  end

  context "CassandraLock#lock" do
    it "waits if the lock is not available" do
      @lock1.lock

      th = Thread.new do
        @lock2.lock
        @scratchpad << :after_lock
      end

      Thread.pass while th.status && th.status != "sleep"

      @scratchpad.should == []
      @lock1.unlock
      th.join
      @scratchpad.should == [:after_lock]
    end
  end

  context "CassandraLock#synchronize" do
    it "raises an ArgumentError when called without a block" do
      lambda { @lock1.synchronize }.should raise_error(ArgumentError)
    end

    it "acquires the lock before yielding and releases it after" do
      th = Thread.new do
        @lock1.synchronize {
          @scratchpad << :after_lock
          sleep 0.5
        }
      end

      Thread.pass while @scratchpad.empty?

      start = Time.now
      @lock2.lock
      time = Time.now - start
      time.should be_close(0.5, 0.2)
    end
  end

  context "CassandraLock#try_lock" do
    it "acquires the lock and returns true if it is available immediately" do
      @lock1.try_lock.should == true
    end

    it "returns false if it cannot acquire the lock immediately" do
      @lock1.lock
      @lock2.try_lock.should == false
    end

    it "removes itself from the waiting line if it cannot acquire the lock" do
      @lock1.lock
      @lock2.try_lock.should == false
      @lock1.unlock
      @lock1.lock
    end
  end

  context "Real-ish test example" do
    it "no holding periods should overlap" do
      threads = []
      holding_periods = []

      1.upto(10).each do |worker_id|
        threads << Thread.new {
          lock = CassandraLock.new("test_lock", worker_id)
          lock.lock
          start = Time.now
          sleep 0.2
          finish = Time.now
          lock.unlock
          holding_periods << [start, finish]
        }
      end

      threads.each{|t| t.join }

      holding_periods.size.should == 10

      holding_periods.each_with_index do |period, idx|
        start, finish = period

        # The holding period we are testing in this iteration
        # would of course compare true against itself.
        all_but_this_one = holding_periods.dup
        all_but_this_one.delete_at(idx)

        overlapping = all_but_this_one.select do |test_start, test_finish|
          range = test_start..test_finish
          range.include?(start) || range.include?(finish)
        end

        overlapping.size.should == 0
      end
    end
  end

end