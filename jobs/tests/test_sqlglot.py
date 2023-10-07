import sqlglot


def test_transpile():
    result = sqlglot.transpile(
        sql="SELECT * FROM test where DAT_ENROLLMENT <= DATE_FORMAT(DATE_ADD(CURRENT_DATE(), INTERVAL -33 DAY), '%Y-%m-%d')",
        read=sqlglot.Dialects.MYSQL,
        write=sqlglot.Dialects.SPARK2
    )

    assert result[0] == "SELECT * FROM test WHERE DAT_ENROLLMENT <= DATE_FORMAT(DATE_ADD(CURRENT_DATE, -33), 'yyyy-MM-dd')"
