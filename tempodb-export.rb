#!/usr/bin/env ruby

#
# Example:
#
# TDB_EXPORT_DIR='export' TDB_START='2013-01-01' TDB_END='2014-12-31' TDB_DATABASE_ID='...' TDB_API_KEY='...' TDB_API_SECRET='...' ruby export.rb
#
#

require 'rubygems'
require 'tempodb'
require 'json'
require 'csv'
require 'fileutils'
require 'time'

export_dir = ENV['TDB_EXPORT_DIR']
start_on = Time.parse(ENV['TDB_START'])
end_on = Time.parse(ENV['TDB_END'])

tdb = TempoDB::Client.new(ENV['TDB_DATABASE_ID'], ENV['TDB_API_KEY'], ENV['TDB_API_SECRET'])

tdb.list_series.each do |series|
  puts "Exporting [#{series.id}]: #{series.key}"

  meta_file = File.join(ENV['TDB_EXPORT_DIR'], series.id, 'meta.json')
  data_file = File.join(ENV['TDB_EXPORT_DIR'], series.id, 'data.csv')

  FileUtils.mkdir_p(File.dirname(meta_file))
  File.open(meta_file, 'w') do |f|
    f.puts series.to_json
  end

  CSV.open(data_file, 'w') do |csv|
    csv << %w[timestamp value]
    cursor = tdb.read_data(series.key, start_on, end_on)
    cursor.each do |dp|
      csv << [dp.ts, dp.value]
    end
  end

end
