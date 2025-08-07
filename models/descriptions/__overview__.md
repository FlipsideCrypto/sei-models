{% docs __overview__ %}

# Welcome to the Flipside Crypto SEI Models Documentation

## **What does this documentation cover?**
The documentation included here details the design of the SEI
 tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/sei-models/)

## **How do I use these docs?**
The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`SEI`.`CORE`.`<table_name>`)
### Defi Tables (`SEI`.`DEFI`.`<table_name>`)
### Governance Tables (`SEI`.`GOV`.`<table_name>`)
### Pricing Tables (`SEI`.`PRICE`.`<table_name>`)
### Statistics/Analytics Tables (`SEI`.`STATS`.`<table_name>`)
### EVM Tables (`SEI`.`CORE_EVM`.`<table_name>`)

**Core Dimension Tables:**
- [dim_address_mapping](#!/model/model.sei_models.core__dim_address_mapping)
- [dim_labels](#!/model/model.sei_models.core__dim_labels)
- [dim_tokens](#!/model/model.sei_models.core__dim_tokens)


**Core Fact Tables:**
- [fact_blocks](#!/model/model.sei_models.core__fact_blocks)
- [fact_msg_attributes](#!/model/model.sei_models.core__fact_msg_attributes)
- [fact_msgs](#!/model/model.sei_models.core__fact_msgs)
- [fact_oracle_votes](#!/model/model.sei_models.core__fact_oracle_votes)
- [fact_transactions](#!/model/model.sei_models.core__fact_transactions)
- [fact_transfers](#!/model/model.sei_models.core__fact_transfers)

**Governance Fact  Tables:**
- [fact_governance_proposal_deposits](#!/model/model.sei_models.gov__fact_governance_proposal_deposits)
- [fact_governance_submit_proposal](#!/model/model.sei_models.gov__fact_governance_submit_proposal)
- [fact_governance_validator_votes](#!/model/model.sei_models.gov__fact_governance_validator_votes)
- [fact_governance_votes](#!/model/model.sei_models.gov__fact_governance_votes)
- [fact_staking](#!/model/model.sei_models.gov__fact_staking)
- [fact_staking_rewards](#!/model/model.sei_models.gov__fact_staking_rewards)

**Defi Fact  Tables:**
- [fact_dex_swaps](#!/model/model.sei_models.defi__fact_dex_swaps)
- [fact_lp_actions](#!/model/model.sei_models.defi__fact_lp_actions)
- [ez_dex_swaps](#!/model/model.sei_models.defi__ez_dex_swaps)

**Pricing Tables:**
- [dim_asset_metadata](#!/model/model.sei_models.price__dim_asset_metadata)
- [fact_prices_ohlc_hourly](#!/model/model.sei_models.price__fact_prices_ohlc_hourly)
- [ez_asset_metadata](#!/model/model.sei_models.price__ez_asset_metadata)
- [ez_prices_hourly](#!/model/model.sei_models.price__ez_prices_hourly)

**Stats EZ Tables:**
- [ez_core_metrics_hourly](#!/model/model.sei_models.ez_core_metrics_hourly)

## EVM Tables (`SEI.CORE_EVM`)
- [fact_blocks](#!/model/model.sei_models.core_evm__fact_blocks)
- [fact_transactions](#!/model/model.sei_models.core_evm__fact_transactions)
- [fact_event_logs](#!/model/model.sei_models.core_evm__fact_event_logs)
- [fact_traces](#!/model/model.sei_models.core_evm__fact_traces)
- [dim_contracts](#!/model/model.sei_models.core_evm__dim_contracts)
- [dim_labels](#!/model/model.sei_models.core_evm__dim_labels)
- [fact_token_transfers](#!/model/model.sei_models.core_evm__fact_token_transfers)
- [ez_token_transfers](#!/model/model.sei_models.core_evm__ez_token_transfers)
- [ez_native_transfers](#!/model/model.sei_models.core_evm__ez_native_transfers)
- [ez_decoded_event_logs](#!/model/model.sei_models.core_evm__ez_decoded_event_logs)
- [sei.nft.ez_nft_transfers](#!/model/model.sei_models.nft__ez_nft_transfers)

## **Data Model Overview**

The SEI
 models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (core/defi/core/prices): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/)
- [Data Studio](https://flipsidecrypto.xyz/edit)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/sei-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}
