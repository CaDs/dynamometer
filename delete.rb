require "rubygems"
require 'aws-sdk'
# If your client is single-threaded, we just need a single connection in our Redis connection pool

require File.expand_path(File.dirname(__FILE__) + '/config/aws_config')
SWIPE_TABLE = YAML.load(File.open(File.join(File.dirname(__FILE__), "/config/dynamodb.yml")))['table']

table = AWS::DynamoDB.new.tables[SWIPE_TABLE]

table.load_schema
table.items.each{|i| i.delete}


