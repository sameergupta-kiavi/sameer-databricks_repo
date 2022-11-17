def snowflake_data_transformation(SCHEMA_NAME):
    return f"""
    -- BORROWER_INFORMATION table
    CREATE OR REPLACE TABLE LH_PROD_DATA_SNOWFLAKE.{SCHEMA_NAME}.borrower_information AS
    -- rental_properties - info about the properties that are used for rental purpose after Oct 2021
    WITH rental_properties AS (
        SELECT
            DISTINCT l.loan_id,
            dd.LOAN_DETAIL_COMPLETEADDRESS AS address,
            l.submitted_at :: date AS loan_submitted_at,
            l.closed_date,
            LAST_VALUE(v1.VALUE_MARKET :: int) OVER (
                PARTITION BY l.loan_id
                ORDER BY
                    v1.CREATED_AT ASC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
            ) AS borrower_estimate,
            CASE
                WHEN a.GEOCODER = 'lob' THEN lower(
                    IFNULL(
                        PARSE_JSON(a.GEOCODER_RESPONSE) :components :zip_code,
                        ''
                    )
                )
                ELSE IFNULL(
                    CASE
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8]) :short_name
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9])\
                             :types [0] :: STRING = 'postal_code'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9]) :short_name
                    END,
                    ''
                )
            END AS zip,
            length(
                split_part(
                    CASE
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8]) :short_name :: STRING
                        WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9])\
                             :types [0] :: STRING = 'subpremise'\
                            THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9]) :short_name :: STRING
                    END,
                    ' ',
                    2
                )
            ) num2,
            CASE
                WHEN a.GEOCODER = 'lob' THEN lower(
                    IFNULL(
                        PARSE_JSON(a.GEOCODER_RESPONSE) :components :secondary_number,
                        ''
                    )
                )
                ELSE IFNULL(
                    CASE
                        WHEN num2 > 0 THEN split_part(
                            CASE
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8]) :short_name :: STRING
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9])\
                                     :types [0] :: STRING = 'subpremise'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9]) :short_name :: STRING
                            END,
                            ' ',
                            2
                        )
                        ELSE CASE
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8]) :short_name :: STRING
                            WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9])\
                                 :types [0] :: STRING = 'subpremise'\
                                THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9]) :short_name :: STRING
                        END
                    END,
                    ''
                )
            END AS unit_number,
            REPLACE (
                CASE
                    WHEN a.GEOCODER = 'lob' THEN trim(
                        IFNULL(
                            PARSE_JSON(a.GEOCODER_RESPONSE) :components :primary_number,
                            ''
                        ) || ' ' || lower(
                            IFNULL(
                                PARSE_JSON(a.GEOCODER_RESPONSE) :components :street_predirection,
                                ''
                            )
                        ) || ' ' || lower(
                            IFNULL(
                                PARSE_JSON(a.GEOCODER_RESPONSE) :components :street_name,
                                ''
                            )
                        ) || ' ' || lower(
                            IFNULL(
                                PARSE_JSON(a.GEOCODER_RESPONSE) :components :street_suffix,
                                ''
                            )
                        ) || ' ' || lower(
                            IFNULL(
                                PARSE_JSON(a.GEOCODER_RESPONSE) :components :street_postdirection,
                                ''
                            )
                        ) || ' ' || lower(
                            IFNULL(
                                PARSE_JSON(a.GEOCODER_RESPONSE) :components :city,
                                ''
                            )
                        ) || ' ' || lower(
                            IFNULL(
                                PARSE_JSON(a.GEOCODER_RESPONSE) :components :zip_code,
                                ''
                            )
                        )
                    )
                    ELSE trim(
                        IFNULL(
                            CASE
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                                     :types [0] :: STRING = 'street_number'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                                     :types [0] :: STRING = 'street_number'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                                     :types [0] :: STRING = 'street_number'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                                     :types [0] :: STRING = 'street_number'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                                     :types [0] :: STRING = 'street_number'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name
                            END,
                            ''
                        ) || ' ' || IFNULL(
                            CASE
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                                     :types [0] :: STRING = 'route'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                                     :types [0] :: STRING = 'route'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                                     :types [0] :: STRING = 'route'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                                     :types [0] :: STRING = 'route'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                                     :types [0] :: STRING = 'route'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name
                            END,
                            ''
                        ) || ' ' || IFNULL(
                            CASE
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                                     :types [0] :: STRING = 'locality'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                                     :types [0] :: STRING = 'locality'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                                     :types [0] :: STRING = 'locality'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                                     :types [0] :: STRING = 'locality'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                                     :types [0] :: STRING = 'locality'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name
                            END,
                            ''
                        ) || ' ' || IFNULL(
                            CASE
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                                     :types [0] :: STRING = 'administrative_area_level_1'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                                     :types [0] :: STRING = 'administrative_area_level_1'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                                     :types [0] :: STRING = 'administrative_area_level_1'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                                     :types [0] :: STRING = 'administrative_area_level_1'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                                     :types [0] :: STRING = 'administrative_area_level_1'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5])\
                                     :types [0] :: STRING = 'administrative_area_level_1'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5]) :short_name
                            END,
                            ''
                        ) || ' ' || IFNULL(
                            CASE
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [0]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [1]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [2]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [3]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [4]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [5]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [6]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [7]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [8]) :short_name
                                WHEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9])\
                                     :types [0] :: STRING = 'postal_code'\
                                    THEN PARSE_JSON(PARSE_JSON(a.GEOCODER_RESPONSE) :address [9]) :short_name
                            END,
                            ''
                        )
                    )
                END,
                '  ',
                ' '
            ) AS normalized_address
        FROM
            mavericks.bridge_loan l
            JOIN "LH_PROD_DATA_SNOWFLAKE"."MAVERICKS"."BRIDGE_LOAN_DETAIL_DEFAULT" dd ON dd.loan_id = l.loan_id
            JOIN lh_ops.properties p ON p.loan_id = l.loan_id
            JOIN lh_ops.addresses a ON p.address_id = a.id
            JOIN lh_ops.valuations v1 ON v1.property_id = p.id
            AND v1.VALUATION_TYPE = 'borrower'
            AND v1.VALUE_MARKET IS NOT NULL
            AND v1.DELETED_AT IS NULL
        WHERE
            l.submitted_at >= '2021-10-01'
            AND l.program_tag = 'rental'
    ),
    -- rural_flag has distinct loan_id from lh_ops.loans dataset.
    rural_flag AS (
        SELECT
            DISTINCT l.id loan_id,
            '' Rural_flag
        FROM
            lh_ops.loans l
    ),
    extra_fields AS (
        SELECT
            DISTINCT la.loan_id AS loan_id,
            la.AUGMENTED_NewOrRepeat AS new_or_repeat,
            la.AUGMENTED_IsReactivation AS is_reactivation
        FROM
            mavericks.bridge_loan bl
            JOIN mavericks.bridge_loan_augmented la ON bl.loan_id = la.loan_id
    )


    SELECT
        fa.*,
        a.*,
        last_value(to_date(dm.RECORDINGDATE, 'YYYYMMDD')) over (
            PARTITION by fa.propertyid
            ORDER BY
                to_date(dm.RECORDINGDATE, 'YYYYMMDD') ROWS BETWEEN unbounded preceding
                AND unbounded following
        ) AS last_sold_date,
        last_value(dm.transactiontype) over (
            PARTITION by fa.propertyid
            ORDER BY
                to_date(dm.RECORDINGDATE, 'YYYYMMDD') ROWS BETWEEN unbounded preceding
                AND unbounded following
        ) AS last_transactiontype,
        last_value(dm.saleamt) over (
            PARTITION by fa.propertyid
            ORDER BY
                to_date(dm.RECORDINGDATE, 'YYYYMMDD') ROWS BETWEEN unbounded preceding
                AND unbounded following
        ) AS last_saleamt,
        last_value(dm.Firstmtgamt) over (
            PARTITION by fa.propertyid
            ORDER BY
                to_date(dm.RECORDINGDATE, 'YYYYMMDD') ROWS BETWEEN unbounded preceding
                AND unbounded following
        ) AS last_mtg_amt,
        rural_flag.rural_flag,
        extra_fields.new_or_repeat,
        extra_fields.is_reactivation
    FROM
        rental_properties a
        LEFT JOIN extra_fields ON extra_fields.loan_id = a.loan_id
        LEFT JOIN "LH_PROD_DATA_SNOWFLAKE"."FIRSTAMERICAN"."FIRSTAMERICAN_ASSESSOR" fa ON fa.SITUSZIP5 = a.zip
        AND (
            (
                CONTAINS(
                    lower(a.normalized_address),
                    lower(
                        CASE
                            WHEN fa.SITUSUNITTYPE IS NOT NULL\
                                 THEN SPLIT_PART(fa.SITUSFULLSTREETADDRESS, fa.SITUSUNITTYPE, 0)
                            ELSE fa.SITUSFULLSTREETADDRESS
                        END
                    )
                )
                AND substr(
                    lower(
                        CASE
                            WHEN fa.SITUSUNITTYPE IS NOT NULL\
                                 THEN SPLIT_PART(fa.SITUSFULLSTREETADDRESS, fa.SITUSUNITTYPE, 0)
                            ELSE fa.SITUSFULLSTREETADDRESS
                        END
                    ),
                    1,
                    5
                ) = substr(lower(a.normalized_address), 1, 5)
            )
        )
        AND IFNULL(fa.SITUSUNITNBR, '') = IFNULL(a.unit_number, '')
        LEFT JOIN "LH_PROD_DATA_SNOWFLAKE"."FIRSTAMERICAN"."DEED_MORTGAGE" dm ON fa.propertyid = dm.propertyid
        LEFT JOIN rural_flag ON rural_flag.loan_id = a.loan_id
        LEFT JOIN scratch.rental_model_loan_hist ON a.loan_id = rental_model_loan_hist.loan_id
    WHERE
        rental_model_loan_hist.loan_id IS NULL;
    """
