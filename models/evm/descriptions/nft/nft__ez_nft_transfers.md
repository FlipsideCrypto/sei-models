{% docs ez_nft_transfers_table_doc %}
This table contains all NFT transfer events for ERC-721 and ERC-1155 tokens on EVM blockchains. It provides a comprehensive view of NFT movements including transfers, mints, and burns.
{% enddocs %}

{% docs ez_nft_transfers_from_address %}

The address sending/transferring the NFT. Special value of '0x0000000000000000000000000000000000000000' indicates minting event.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_nft_transfers_to_address %}

The address receiving the NFT. Special value of '0x0000000000000000000000000000000000000000' indicates burning event.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_nft_transfers_token_id %}

The unique identifier for a specific NFT within a collection. String format to handle various token_id formats.

Example: '1234'

{% enddocs %}

{% docs ez_nft_transfers_intra_event_index %}

Position within a batch transfer event, primarily for ERC-1155. Always starts with 1 for single transfers.

Example: 1

{% enddocs %}

{% docs ez_nft_transfers_nft_quantity %}

The number of NFTs transferred for this specific token_id. Always 1 for ERC-721, can be more for ERC-1155.

Example: 1

{% enddocs %}

{% docs ez_nft_transfers_token_transfer_type %}

The specific event type emitted by the contract. Values include 'erc721_Transfer', 'erc1155_TransferSingle', 'erc1155_TransferBatch', etc.

Example: 'erc721_Transfer'

{% enddocs %}

{% docs ez_nft_transfers_is_mint %}

Boolean flag indicating if this transfer is a minting event (from address is 0x0).

Example: true

{% enddocs %}

{% docs ez_nft_transfers_contract_address %}

The address of the contract that emitted the NFT transfer event.

Example: '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'

{% enddocs %}

{% docs ez_nft_transfers_name %}

The name of the NFT collection. For Ethereum only, join with nft.dim_nft_collection_metadata for token-level details.

Example: 'Bored Ape Yacht Club'

{% enddocs %}

{% docs ez_nft_transfers_token_standard %}

The standard of the NFT. Values include 'erc721', 'erc1155', 'cryptopunks', and 'legacy'.

Example: 'erc721'

{% enddocs %}