import datapkg


def test_parse_connection_string():
    connection_string = (
        'mysql://root:root_pass@192.168.0.1:3306/test'
    )
    output = {
        'db_name': 'test',
        'db_type': 'mysql',
        'host_ip': '192.168.0.1',
        'host_port': '3306',
        'password': 'root_pass',
        'socket': '',
        'username': 'root',
    }
    assert datapkg.parse_connection_string(connection_string) == output
    assert datapkg.make_connection_string(**output) == connection_string

    connection_string = (
        'mysql://root:root_pass@192.168.0.1:3306/test?unix_socket=/tmp/mysql.sock'
    )
    output = {
        'db_name': 'test',
        'db_type': 'mysql',
        'host_ip': '192.168.0.1',
        'host_port': '3306',
        'password': 'root_pass',
        'socket': '/tmp/mysql.sock',
        'username': 'root',
    }
    assert datapkg.parse_connection_string(connection_string) == output
    assert datapkg.make_connection_string(**output) == connection_string
