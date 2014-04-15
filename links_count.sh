mysql -uworker -pworker -Dyaopin -e 'select count(url_no) from link_info where host_no = 4;'
mysql -uworker -pworker -Dyaopin -e 'select count(url_no) from link_info where host_no = 4 and check_time > 0;'
