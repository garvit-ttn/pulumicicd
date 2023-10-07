import json

from cvmdatalake import get_s3_path_for_feature, features


def test_get_feature_s3_path_with_context():
    lake_descriptor = {
        'features.clv.input.train': 's3://test/clv/train'
    }

    path = get_s3_path_for_feature(features.ClvTrainingInput, json.dumps(lake_descriptor))
    assert str(path) == 's3://test/features/clv/train/input/train/default.all'
