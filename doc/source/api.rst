Cyanite HTTP Service
====================

The Cyanite API is responsible for exposing an HTTP service to service queries.
The Cyanite API exposes the following HTTP routes:

- ``/ping``: report online status
- ``/metrics``: query metrics.  Takes ``from``, ``to`` (optional), and any number of ``path`` arguments.
- ``/paths``: query paths.  Takes a ``query`` argument.

Authentication
--------------

Cyanite provides no authentication means.

Authorization
-------------

Cyanite provides no authorization methods.

Routes
------

``/ping``
~~~~~~~~~

Report online status.

Input
    Takes no parameter

Output
    .. sourcecode:: json
                    
       {
         "response": "pong"
       }                


``/paths``
~~~~~~~~~~

Query available metric paths.

Input
    - ``query``: A valid path query

Output
    .. sourcecode:: json
                    
       {
         "paths": [
           "path1",
           "path2",
           "pathN"
         ]
       }


``/metrics``
~~~~~~~~~~~~

Query metric time-series

Input
    - ``from``: Timestamp at which to start query.
    - ``to``: Optional timestamp at which to stop querying.
      Assume wall-clock time if unspecified.
    - ``path``: May be supplied several times. Path or path
      query to retrieve.

Output
    .. sourcecode:: json
                    
       {
        "from"  : 1437572670,
        "to"    : 1437572690,
        "step"  : 10,
        "series": { "web01.cpu": [ 30.0, 40.0, 50.0 ]
       }
    
