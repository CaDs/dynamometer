require 'sinatra'
require './miner.rb'

get	'/test' do
	miner = Miner.new()
	miner.test
end