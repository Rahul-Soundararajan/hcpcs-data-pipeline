Create Database HCPCS;

Use HCPCS;

CREATE TABLE hcpcs_codes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    group_code CHAR(1) NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    hcpcs_code VARCHAR(10) NOT NULL,
    long_description TEXT NOT NULL,
    effective_date DATE DEFAULT (CURRENT_DATE),
    end_date DATE,
    Is_current CHAR(1) NOT NULL
);