{{ config (
    materialized = "view",
    tags = ['streamline_core_evm_complete']
) }}

SELECT
    _id AS block_number,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS block_number_hex
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            MAX(block_number)
        FROM
            {{ ref('streamline__evm_chainhead') }}
    )
    AND _id >= 79123881
ORDER BY
    _id ASC
