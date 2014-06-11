import mixpanel

test = mixpanel.Mixpanel(api_key='da5111c21cb81744b6bb94d7ca4440ef', api_secret='d656bebfbb741756682be46b2168d191')
raw_data = test.event_export({"from_date":"2014-04-01", "to_date":"2014-05-30"}, debug=1, high_volume=0)
test.people_export()
segmentation_data = test.segmentation({"from_date":"2014-04-15", "to_date":"2014-04-30", "event":"Event 1"}, debug=0)
mixpanel.csv(raw_data)
mixpanel.csv(segmentation_data)
