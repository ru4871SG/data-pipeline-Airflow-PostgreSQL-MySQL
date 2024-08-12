-- Insert initial records into the 'staging' table in staging area in PostgreSQL

INSERT INTO staging (
    transaction_id, transaction_date, sales_outlet_id, staff_id, customer_id, 
    sales_detail_id, product_id, quantity, price, staff_first_name, staff_last_name, 
    staff_position, staff_location, sales_outlet_type, outlet_address, outlet_city, 
    outlet_telephone, outlet_postal_code, outlet_manager, customer_name, customer_email, 
    card_number, gender_desc, product_name, description, product_price, product_type_id, 
    product_type, product_category, customer_gender_id, city_id, month_id, year, 
    day_of_week_id, day_of_week, month_name, year_id, date_id
) VALUES
(1, '2019-04-27', 8, 42, 0, 1, 38, 2, 3.75, 'Kylie', 'Candace', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, NULL, NULL, NULL, NULL, 'Latte', 'You will think you are in Venice when you sip this one.', 3.75, 18, 'Barista Espresso', 'Coffee', 3, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(2, '2019-04-27', 8, 42, 0, 3, 51, 2, 3.0, 'Kylie', 'Candace', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, NULL, NULL, NULL, NULL, 'Earl Grey Lg', 'Tradition in a cup.', 3.0, 21, 'Brewed Black tea', 'Tea', 3, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(3, '2019-04-27', 8, 42, 0, 4, 33, 1, 3.5, 'Kylie', 'Candace', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, NULL, NULL, NULL, NULL, 'Ethiopia Lg', 'A bold cup when you want that something extra.', 3.5, 16, 'Gourmet brewed coffee', 'Coffee', 3, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(4, '2019-04-27', 8, 42, 8231, 5, 27, 1, 3.5, 'Kylie', 'Candace', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, 'Finn', 'Jackson@Morbi.us', '978-810-4694', 'M', 'Brazilian Lg', 'It''s like Carnival in a cup. Clean and smooth.', 3.5, 15, 'Organic brewed coffee', 'Coffee', 2, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(5, '2019-04-27', 8, 45, 8222, 6, 24, 1, 3.0, 'Pandora', 'Neville', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, 'Meghan', 'Lavinia@Donec.gov', '348-275-0249', 'F', 'Our Old Time Diner Blend Lg', 'An honest cup a coffee.', 3.0, 14, 'Drip coffee', 'Coffee', 1, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(6, '2019-04-27', 8, 45, 8334, 7, 47, 2, 3.0, 'Pandora', 'Neville', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, 'Cyrus', 'Yardley@luctus.net', '284-461-8662', 'M', 'Serenity Green Tea Lg', 'Feel the stress leaving your body.', 3.0, 20, 'Brewed Green tea', 'Tea', 2, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(7, '2019-04-27', 8, 42, 8099, 8, 57, 1, 3.1, 'Kylie', 'Candace', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, 'Amity', 'Gannon@sem.net', '146-428-0486', 'F', 'Spicy Eye Opener Chai Lg', 'When you need your eyes opened wide.', 3.1, 22, 'Brewed Chai tea', 'Tea', 1, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(8, '2019-04-27', 8, 15, 0, 9, 47, 2, 3.0, 'Remedios', 'Mari', 'Coffee Wrangler', 4, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, NULL, NULL, NULL, NULL, 'Serenity Green Tea Lg', 'Feel the stress leaving your body.', 3.0, 20, 'Brewed Green tea', 'Tea', 3, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(9, '2019-04-27', 8, 45, 0, 10, 30, 2, 3.0, 'Pandora', 'Neville', 'Coffee Wrangler', 10, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, NULL, NULL, NULL, NULL, 'Columbian Medium Roast Lg', 'A smooth cup of coffee any time of day.', 3.0, 16, 'Gourmet brewed coffee', 'Coffee', 3, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27),
(10, '2019-04-27', 8, 15, 0, 11, 56, 2, 2.55, 'Remedios', 'Mari', 'Coffee Wrangler', 4, 'retail', '687 9th Avenue', 'New York', '652-212-7020', '10036', 31, NULL, NULL, NULL, NULL, 'Spicy Eye Opener Chai Rg', 'When you need your eyes opened wide.', 2.55, 22, 'Brewed Chai tea', 'Tea', 3, 2, 4, 2019, 7, 'Saturday', 'April', 1, 27);
