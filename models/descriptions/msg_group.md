{% docs msg_group %}

Value grouping different messages together to represent a single action. Format will include the numeric msg_group and msg_sub_group with a ":" seperator. The subgroup will always be 0 except for "Exec" actions. NULL group means messages are related to the header (overall transaction)

{% enddocs %}