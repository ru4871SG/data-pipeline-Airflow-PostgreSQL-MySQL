-- Only update columns with string values, date and integer columns do not need to be updated
-- This script will make all cells with empty string values to become NULL in your MySQl table

UPDATE `transaction_data`
SET 
    `staff_first_name` = NULLIF(`staff_first_name`, ''),
    `staff_last_name` = NULLIF(`staff_last_name`, ''),
    `staff_position` = NULLIF(`staff_position`, ''),
    `staff_location` = NULLIF(`staff_location`, ''),
    `sales_outlet_type` = NULLIF(`sales_outlet_type`, ''),
    `outlet_address` = NULLIF(`outlet_address`, ''),
    `outlet_city` = NULLIF(`outlet_city`, ''),
    `outlet_telephone` = NULLIF(`outlet_telephone`, ''),
    `customer_name` = NULLIF(`customer_name`, ''),
    `customer_email` = NULLIF(`customer_email`, ''),
    `card_number` = NULLIF(`card_number`, ''),
    `gender_desc` = NULLIF(`gender_desc`, ''),
    `product_name` = NULLIF(`product_name`, ''),
    `description` = NULLIF(`description`, ''),
    `product_type` = NULLIF(`product_type`, ''),
    `product_category` = NULLIF(`product_category`, ''),
    `day_of_week` = NULLIF(`day_of_week`, ''),
    `month_name` = NULLIF(`month_name`, '')
WHERE 
    `staff_first_name` = '' OR
    `staff_last_name` = '' OR
    `staff_position` = '' OR
    `staff_location` = '' OR
    `sales_outlet_type` = '' OR
    `outlet_address` = '' OR
    `outlet_city` = '' OR
    `outlet_telephone` = '' OR
    `customer_name` = '' OR
    `customer_email` = '' OR
    `card_number` = '' OR
    `gender_desc` = '' OR
    `product_name` = '' OR
    `description` = '' OR
    `product_type` = '' OR
    `product_category` = '' OR
    `day_of_week` = '' OR
    `month_name` = '';
