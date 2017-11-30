# frozen_string_literal: true

require 'roo'
require 'main'
require 'sequel'
require 'csv'
require 'logger'

# Interpolate values
class Interpolator
  def load(sheet, table)
    (2..sheet.last_row).collect do |row|
      table.insert date: sheet.cell('A', row),
                   site: sheet.cell('B', row),
                   treatment: sheet.cell('C', row),
                   replicate: sheet.cell('D', row),
                   crop: sheet.cell('E', row),
                   no3: sheet.cell('F', row), tdn: sheet.cell('G', row),
                   doc: sheet.cell('H', row), tdp: sheet.cell('I', row)
    end
  end

  def setup_data_table(db)
    db.create_table :data do
      Date :date
      String :site
      String :treatment
      String :replicate
      String :crop
      Float :no3
      Float :tdn
      Float :doc
      Float :tdp
    end

    db[:data]
  end

  def setup_result_table(db)
    db.create_table :results do
      Date :date
      String :site
      String :treatment
      String :replicate
      String :crop
      Float :no3
      Float :tdn
      Float :doc
      Float :tdp
    end
    db[:results]
  end

  # def record_present?(results, a)
  #   results.where(date: a[:date],
  #                 site: a[:site],
  #                 treatment: a[:treatment],
  #                 replicate: a[:replicate],
  #                 crop: a[:crop]).first
  # end

  def interpolated_record_present?(results, a, day)
    results.where(date: a[:date] + day,
                  site: a[:site],
                  treatment: a[:treatment],
                  replicate: a[:replicate],
                  crop: a[:crop]).first
  end

  def insert_interpolated(results, a, day, values)
    if interpolated_record_present?(results, a, day)
      results.where(date: a[:date] + day, site: a[:site],
                    treatment: a[:treatment], replicate: a[:replicate],
                    crop: a[:crop]).update(values)
    else
      h = { date: a[:date] + day, site: a[:site],
            treatment: a[:treatment], replicate: a[:replicate],
            crop: a[:crop] }.merge(values)
      results.insert h
    end
  end

  # def insert(results, a)
  #   results.insert a unless record_present?(results, a)
  # end

  def interpolate_variable(table, results, plots, variable)
    plots.each do |plot|
      data = table.where(plot).exclude(variable => nil).order(:date).all

      data.each_cons(2) do |a, b|
        # insert first value
        next if a[:date] + 1 == b[:date]
        # compute fit
        days = (b[:date] - a[:date]).to_i
        diff = b[variable] - a[variable]
        slope = diff / days
        # evaluate for each missing value
        (0..days).each do |day|
          p [a[variable], diff, days, slope, day, a[variable] + slope * day]
          insert_interpolated(results, a, day,
                              variable => a[variable] + slope * day)
        end
      end
      # insert(results, data.last)
    end
  end

  def interpolate(file)
    File.delete('data.sqlite') if File.exist?('data.sqlite')
    db = Sequel.sqlite('data.sqlite', loggers: [Logger.new($stdout)])

    s = Roo::Spreadsheet.open(file)
    sheet = s.sheet(0)

    table = setup_data_table(db)
    results = setup_result_table(db)

    load(sheet, table)

    plots = db['select distinct site, treatment, replicate, crop from data'].all
    interpolate_variable(table, results, plots, :no3)
    interpolate_variable(table, results, plots, :tdn)
    interpolate_variable(table, results, plots, :doc)
    interpolate_variable(table, results, plots, :tdp)
    # puts results.to_csv
    exit
  end
end

Main do
  argument 'file'

  def run
    i = Interpolator.new
    i.interpolate(params['file'].value)
  end
end
