import pulumi_aws as aws


def create_offers_table(environment: str) -> aws.dynamodb.Table:
    return aws.dynamodb.Table(
        f"cvm-{environment}-offers-dynamodb-table",
        billing_mode="PAY_PER_REQUEST",
        name="cvm-offers",
        hash_key="idi_offer",
        attributes=[
            aws.dynamodb.TableAttributeArgs(name="idi_offer", type="S"),
        ],
    )


def create_default_eligible_offers_table(environment: str) -> aws.dynamodb.Table:
    return aws.dynamodb.Table(
        f"cvm-{environment}-default-eligible-offers-dynamodb-table",
        billing_mode="PAY_PER_REQUEST",
        name="cvm-default-eligible-offers",
        hash_key="idi_offer",
        attributes=[
            aws.dynamodb.TableAttributeArgs(name="idi_offer", type="S"),
        ],
        ttl=aws.dynamodb.TableTtlArgs(
            attribute_name='expires_at',
            enabled=True
        ),
    )


def create_share_table(environment: str) -> aws.dynamodb.Table:
    return aws.dynamodb.Table(
        f"cvm-{environment}-share-dynamodb-table",
        billing_mode="PAY_PER_REQUEST",
        name="cvm-share",
        hash_key="share_id",
        range_key="cvm_campaign_name",
        attributes=[
            aws.dynamodb.TableAttributeArgs(name="share_id", type="S"),
            aws.dynamodb.TableAttributeArgs(name="cvm_campaign_name", type="S"),
        ],
    )


def create_braze_table(environment: str) -> aws.dynamodb.Table:
    return aws.dynamodb.Table(
        f"cvm-{environment}-braze-dynamodb-table",
        billing_mode="PAY_PER_REQUEST",
        name="cvm-braze",
        hash_key="braze_id",
        range_key="cvm_campaign_name",
        attributes=[
            aws.dynamodb.TableAttributeArgs(name="braze_id", type="S"),
            aws.dynamodb.TableAttributeArgs(name="cvm_campaign_name", type="S"),
        ],
    )


def create_unified_offers_table(environment: str, suffix: str | None = None):
    return aws.dynamodb.Table(
        f"cvm-{environment}-unified-offers" if suffix is None else f"cvm-{environment}-unified-offers-{suffix}",
        billing_mode="PAY_PER_REQUEST",
        name="cvm-unified-offers" if suffix is None else f"cvm-unified-offers-{suffix}",
        hash_key="gcr_id",
        range_key="rank",
        attributes=[
            aws.dynamodb.TableAttributeArgs(name="gcr_id", type="S"),
            aws.dynamodb.TableAttributeArgs(name="rank", type="N"),
            aws.dynamodb.TableAttributeArgs(name="share_id", type="S"),
            aws.dynamodb.TableAttributeArgs(name="braze_id", type="S"),
            aws.dynamodb.TableAttributeArgs(name="crf_id", type="S"),
        ],
        global_secondary_indexes=[
            aws.dynamodb.TableGlobalSecondaryIndexArgs(
                name="braze_id-rank-index",
                hash_key="braze_id",
                range_key="rank",
                projection_type="ALL",
            ),
            aws.dynamodb.TableGlobalSecondaryIndexArgs(
                name="crf_id-rank-index",
                hash_key="crf_id",
                range_key="rank",
                projection_type="ALL",
            ),
            aws.dynamodb.TableGlobalSecondaryIndexArgs(
                name="share_id-rank-index",
                hash_key="share_id",
                range_key="rank",
                projection_type="ALL",
            ),
        ],
    )
