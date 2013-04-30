require './spec/spec_helper'

describe Miner do
	before(:each) do
	  YAML.stub(:load).and_return({"host"=>"127.0.0.1", "port"=>6379})
		@miner = Miner.new
	end
	after(:each) do
		redis = @miner.setup_redis
	  redis.del(Miner::REDIS_LOCK) if redis
	end

	context "Redis Setup" do
	  it "should read the config file and create a redis conection" do
	    redis = @miner.setup_redis
	    redis.client.host.should == "127.0.0.1"
	    redis.client.port.should == 6379
	    redis.ping.should == "PONG"
	  end

  	it "should return false if there is any problem with the connection with Redis" do
    	Redis.stub(:new).and_raise(Exception.new("Connection Exception"))
    	@miner.setup_redis.should be_false
  	end
	end

	context "DynamoDB Execution" do
		after(:each) do
			redis = Miner.new.setup_redis
			if redis
				redis.del('test_queue')
				redis.del(Miner::REDIS_LOCK)
			end
		end
		it "should do nothing if the redis client is down" do
		  Redis.stub(:new).and_raise(Exception.new("Connection Exception"))
		  AWS::DynamoDB.should_not_receive(:new)
		  Miner.should_receive(:reschedule).exactly(1).times.and_return(true)
		  @miner.perform('test_queue')
		end

		it "shold push to DynamoDB a block of swipes" do
			redis = @miner.setup_redis
			redis.del(Miner::REDIS_LOCK)
			push_sample_data(redis)

			fake_connector = mock(AWS::DynamoDB)
			fake_table = mock(AWS::DynamoDB::Table)
			fake_connector.stub(:tables).and_return(fake_table)
			fake_table.stub('[]').and_return(fake_table)
			fake_table.stub(:batch_put).and_return(true)
			AWS::DynamoDB.should_receive(:new).exactly(1).times.and_return(fake_connector)
			@miner.perform('test_queue')
		end

		it "should do nothing if the conection with dinamo can't be created" do
			redis = @miner.setup_redis
			push_sample_data(redis)
		  AWS::DynamoDB.stub(:new).and_raise(Exception.new("Can't connect with Dynamo"))
		  Miner.should_receive(:reschedule).exactly(1).times.and_return(true)
		  @miner.perform('test_queue')
		end
	end

	context "check if Redis is available" do
	  it "should lock redis" do
	  	redis = @miner.setup_redis
	    @miner.lock_redis(redis)
	    redis.get(Miner::REDIS_LOCK).to_s.should == "1"
	    @miner.free_to_go(redis).should be_false
	  end

	  it "should unlock redis" do
	    redis = @miner.setup_redis
	    @miner.unlock_redis(redis)
	    redis.get(Miner::REDIS_LOCK).to_s.should == "0"
	    @miner.free_to_go(redis).should be_true
	  end
	end

	def push_sample_data(redis)
    swipes = [{"s_pos"=>"318,364", "e_pos"=>"318,364", "t_time"=>6, "m_g"=>"h", :swipe_id=>"b7f962c8-5fd3-4329-aeb4-2e97b38047f1"},
              {"s_pos"=>"432,448", "e_pos"=>"432,448", "t_time"=>0, "m_g"=>"h", :swipe_id=>"1defc0ac-5a1b-495a-b56a-1500c437ea49"},
              {"s_pos"=>"120,403", "e_pos"=>"120,403", "t_time"=>6, "m_g"=>"h", :swipe_id=>"98f84a75-e414-4ad4-8c2f-f9aad2fa3ce6"},
              {"s_pos"=>"266,386", "e_pos"=>"266,386", "t_time"=>5, "m_g"=>"h", :swipe_id=>"156e6895-c09d-4a5b-a15a-abf8b35f98b2"},
              {"s_pos"=>"486,296", "e_pos"=>"486,296", "t_time"=>6, "m_g"=>"h", :swipe_id=>"6175ddbe-8d40-4d75-8c49-cadf57a4af09"}]
    swipes.each{|swipe| redis.lpush('test_queue', swipe)}
  end

end