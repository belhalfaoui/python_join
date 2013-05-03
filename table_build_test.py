from python_join import TableBuilder

tb = TableBuilder(
		main_db = 'platform',
		main_query = """
			SELECT
				booking_related_activity.booking_id,
				booking_ref,
				bookingsinfo.property_id,
				date_from,
				date_to,
				activity.requested_at,
				activity.user_id,
				arrived_at_home.completed_at AS arrived,
				guest_arrived.completed_at AS guest_arrived,
				keys_to_guest.completed_at AS keys_to_guest,
				REPLACE(REPLACE(REPLACE(notes.task_detail_json, '}', ')'), '", ', ' ('),'{"notes": "','') AS notes
			FROM booking_related_activity
			JOIN bookings ON booking_related_activity.booking_id=bookings.id
			JOIN bookingsinfo ON bookings.tail_info_id=bookingsinfo.id
			JOIN activity ON activity.id=booking_related_activity.activity_id
			LEFT OUTER JOIN activity_task AS arrived_at_home ON arrived_at_home.task_id=3 AND arrived_at_home.activity_id=activity.id  
			LEFT OUTER JOIN activity_task AS guest_arrived ON guest_arrived.task_id=10 AND guest_arrived.activity_id=activity.id
			LEFT OUTER JOIN activity_task AS keys_to_guest ON keys_to_guest.task_id=18 AND keys_to_guest.activity_id=activity.id
			LEFT OUTER JOIN activity_task AS notes ON notes.task_detail_json LIKE '{"notes"%' AND notes.activity_id=activity.id
		""",
		create_query = """
			CREATE TABLE `Meet_greet_test` (
			`activity_id` INT (11),
			`booking_ref` VARCHAR (135),
			`property_id` INT (11),
			`short_code` VARCHAR (30),
			`date_from` DATETIME ,
			`date_to` DATETIME ,
			`requested_at` DATETIME ,
			`user_id` INT (11),
			`greeter_name` VARCHAR (183),
			`greeter_email` VARCHAR (225),
			`arrived` DATETIME ,
			`guest_arrived` DATETIME ,
			`keys_to_guest` DATETIME ,
			`notes` TEXT,
			PRIMARY KEY (`activity_id`),
			KEY `booking_ref` (`booking_ref`),
			KEY `short_code` (`short_code`)
		)
		""",
		destination_table = 'Meet_greet_test',
		verbose = True
)

tb.build()

tb.add_source('short_codes', 'ecom', "SELECT id, short_code FROM property_unit WHERE short_code IS NOT NULL", join_on=2, outer_join=True, keep_key_column=True)
tb.add_source('greeter_info', 'ecom', "SELECT id, CONCAT(first_name, ' ', last_name) AS greeter_name, email AS greeter_email FROM auth_user", join_on=6, outer_join=True, keep_key_column=True)

tb.join()
tb.write()
tb.reporting()