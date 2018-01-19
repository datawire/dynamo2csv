# scout2csv

Scout data export tool.

# Install

Just install from source...

`pip3 install -U git+git://github.com/datawire/scout2csv.git`

# CLI Usage

All apps:

`scout2csv export -t scout YYYY-MM-DD YYYY-MM-DD`

Single app:

`scout2csv export -t scout YYYY-MM-DD YYYY-MM-DD --app=ambassador`

## About Date Ranges

The date ranges is exclusive for `<start_date>` and exclusive for `<end_date>` this means that given a query such as 
`scout2csv export -t scout 2018-01-01 2018-01-07` what will happen is records from `2018-01-01 12:00:00AM` to 
`2018-01-07 12:00:00AM` will be exported. This means generally you need increment the `<end_date>` day by '1' to ensure
you get all the records you want.  