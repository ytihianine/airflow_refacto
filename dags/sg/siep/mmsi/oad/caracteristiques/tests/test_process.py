from dags.sg.siep.mmsi.oad.tests.fixtures import sample_dataframe

from dags.sg.siep.mmsi.oad.process import rename_columns


def test_rename_columns(sample_dataframe):
    sample_dataframe = rename_columns(df=sample_dataframe)
