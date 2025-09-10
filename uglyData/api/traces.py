from contextlib import contextmanager
import elasticapm


@contextmanager
def start_transaction(name: str, ttype: str):
    """
    Start a transaction

    Parameters
    ----------
    name : str
        Name of the transaction
    ttype : str
        Type of the transaction

    Returns
    -------
    contextmanager
        Context manager to be used with the `with` statement
    """
    elasticapm.instrument()
    client = elasticapm.get_client()
    client.begin_transaction(transaction_type=ttype)
    elasticapm.set_transaction_name(name)

    try:
        yield client
    except Exception as e:
        client.end_transaction("FAILURE")
        raise e
    else:
        client.end_transaction("SUCCESS")
