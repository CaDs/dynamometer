require "rubygems"
require "bundler/setup"
require 'sidekiq'
require 'redis'
require 'json'
require 'securerandom.rb'
# If your client is single-threaded, we just need a single connection in our Redis connection pool
Sidekiq.configure_client do |config|
  config.redis = { :namespace => 'dynamometer', :size => 1, :url => 'redis://127.0.0.1:6379' }
end

# Sidekiq server is multi-threaded so our Redis connection pool size defaults to concurrency (-c)
Sidekiq.configure_server do |config|
  config.redis = { :namespace => 'dynamometer', :url => 'redis://127.0.0.1:6379' }
end

class Miner
	require File.expand_path(File.dirname(__FILE__) + '/config/aws_config')
  SWIPE_TABLE = YAML.load(File.open(File.join(File.dirname(__FILE__), "/config/dynamodb.yml")))['table']
  include Sidekiq::Worker
  REDIS_QUEUE = "swipe_queue"
  REDIS_LOCK = "lock_enabled"

  def perform(sleep_time=1, throttle=5, rescheduling_time=300, queue_name=REDIS_QUEUE)
    begin
      redis = setup_redis
      return unless free_to_go(redis)
      until redis.llen(queue_name) == 0 do
        sleep sleep_time #Keep under the limit 5 writes per second
        to_push = throttle.times.inject([]){|array, i| array << (JSON.parse(redis.lpop(queue_name)).merge(:swipe_id => SecureRandom.uuid) rescue nil)}
        puts "INFO: About to push #{to_push}.compact"
        AWS::DynamoDB.new.tables[SWIPE_TABLE].batch_put(to_push.compact)
      end
    rescue Exception => e
      puts "Something wrong happended #{e}"
    ensure
      unlock_redis(redis)
      Miner.reschedule(rescheduling_time)
    end
  end

  def setup_redis
    begin
      config_file = File.open(File.join(File.dirname(__FILE__), "/config/redis_config.yml"))
      redis_conf = YAML.load(config_file)
      return Redis.new(:host => redis_conf["host"], :port => redis_conf["port"])
    rescue Exception => e
      return false
    end
  end

  def self.reschedule(time=300)
    #Execute again in 5 minutes
    puts "Reescheduling!!!"
    return Miner.perform_in(time)
  end

  def free_to_go(redis)
    return (redis && check_if_unlocked(redis))
  end

  def lock_redis(redis)
    redis.set(REDIS_LOCK, "1") rescue false
  end

  def unlock_redis(redis)
    redis.set(REDIS_LOCK, "0") rescue false
  end

  def check_if_unlocked(redis)
    (redis.get(REDIS_LOCK).to_s == "" || redis.get(REDIS_LOCK).to_s == "0") rescue false
  end

  def self.startup
    scheduled_jobs = Sidekiq.redis{|c| c.zcard('schedule')}
    puts "WE HAVE #{scheduled_jobs} jobs scheduled"
    Sidekiq.redis{|c| c.del('schedule')} if scheduled_jobs > 1
    Miner.perform_async
  end
end

Miner.startup