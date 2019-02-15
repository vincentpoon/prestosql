=================
Phoenix Connector
=================

The Phoenix connector allows querying data stored in Apache Phoenix.

Compatibility
-------------

The Phoenix connector is compatible with all Phoenix versions starting from 4.14.x.

Configuration
-------------

To configure the Phoenix connector, create a catalog properties file
``etc/catalog/phoenix.properties`` with the following contents,
replacing ``host1,host2,host3`` with a comma-separated list of the ZooKeeper
nodes used for discovery of the hbase cluster:

.. code-block:: none

    connector.name=phoenix
    connection-url=jdbc:phoenix:host1,host2,host3:2181:/hbase
    connection-properties=phoenix.schema.isNamespaceMappingEnabled=true;phoenix.query.timeoutMs=60000

Note that isNamespaceMappingEnabled must be true to create schemas from Presto.

Configuration Properties
------------------------

The following configuration properties are available:

================================================== ====================== ========== ===================================================================================
Property Name                                      Default Value          Required   Description
================================================== ====================== ========== ===================================================================================
``connection-url``                                 (none)                 Yes        ``jdbc:phoenix[:zk_quorum][:zk_port][:zk_hbase_path]``.
                                                                                     The ``zk_quorum`` is a comma separated list of the ZooKeeper Servers.
                                                                                     The ``zk_port`` is the ZooKeeper port. The ``zk_hbase_path`` is the HBase
                                                                                     root znode path, that is configurable using hbase-site.xml.  By
                                                                                     default the location is “/hbase”
``connection-properties``                          (none)                 No         Phoenix connection properties in key=value format, delimited by semicolon
                                                                                     e.g ``phoenix.schema.isNamespaceMappingEnabled=true;phoenix.query.timeoutMs=60000``
================================================== ====================== ========== ===================================================================================

Querying Phoenix Tables
-------------------------

The Phoenix connector provides a schema for every Phoenix schema.
You can see the available Phoenix schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM phoenix;

If you have a Phoenix schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM phoenix.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE phoenix.web.clicks;
    SHOW COLUMNS FROM phoenix.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM phoenix.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``phoenix`` in the above examples.

Data types
----------

The data types mappings are as follows:

==========================   ============
Phoenix                      Presto
==========================   ============
``BOOLEAN``                  ``BOOLEAN``
``BIGINT``                   ``BIGINT``
``INTEGER``                  ``INTEGER``
``SMALLINT``                 ``SMALLINT``
``TINYINT``                  ``TINYINT``
``DOUBLE``                   ``DOUBLE``
``REAL``                     ``FLOAT``
``DECIMAL``                  ``DECIMAL``
``BINARY``                   ``VARBINARY``
``VARBINARY``                ``VARBINARY``
``DATE``                     ``DATE``
``TIME``                     ``TIME``
``TIMESTAMP``                ``TIMESTAMP``
``ARRAY``                    ``ARRAY``
``VARCHAR``                  ``VARCHAR``
``CHAR``                     ``CHAR``
==========================   ============

|

============================   =============
Presto                         Phoenix
============================   =============
``BOOLEAN``                    ``BOOLEAN``
``BIGINT``                     ``BIGINT``
``INTEGER``                    ``INTEGER``
``SMALLINT``                   ``SMALLINT``
``TINYINT``                    ``TINYINT``
``DOUBLE``                     ``DOUBLE``
``FLOAT``                      ``REAL``
``DECIMAL``                    ``DECIMAL``
``VARBINARY``                  ``VARBINARY``
``DATE``                       ``DATE``
``TIME``                       ``TIME``
``TIME_WITH_TIME_ZONE``        ``TIME``
``TIMESTAMP``                  ``TIMESTAMP``
``TIMESTAMP_WITH_TIME_ZONE``   ``TIMESTAMP``
``ARRAY``                      ``ARRAY``
``VARCHAR``                    ``VARCHAR``
``CHAR``                       ``CHAR``
============================   =============

Table Properties
----------------

Table property usage example:

.. code-block:: sql

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      birthday DATE
      name VARCHAR,
      age BIGINT
    )
    WITH (
      rowkeys = 'recordkey,birthday',
      salt_buckets=10
    );

=========================== ================ ==============================================================================================================
Property Name               Default Value    Description
=========================== ================ ==============================================================================================================
``rowkeys``                 (ROWKEY column)  Comma-delimited list of columns to be the primary key in the Phoenix table.
                                             If not specified, a 'ROWKEY' column is generated.

``salt_buckets``            (none)           ``salt_buckets`` numeric property causes an extra byte to be transparently prepended to every row key
                                             to ensure an evenly distributed read and write load across all region servers.

``split_on``                (none)           Per-split table Salting does automatic table splitting but in case you want to exactly control where
                                             table split occurs with out adding extra byte or change row key order then you can pre-split a table.

``disable_wal``             false            ``disable_wal`` boolean option when true causes HBase not to write data to the write-ahead-log, thus
                                             making updates faster at the expense of potentially losing data in the event of a region server failure.

``immutable_rows``          false            ``immutable_rows`` boolean option when true declares that your table has rows which are write-once,
                                             append-only (i.e. the same row is never updated).

``default_column_family``   ``0``            ``default_column_family`` string option determines the column family used used when none is specified.
                                             The value is case sensitive.

``bloomfilter``             ``ROW``          ``bloomfilter`` are enabled on a Column Family. Valid values are ``NONE``, ``ROW``(default), or ``ROWCOL``.

``versions``                ``1``            A ``{row, column, version}`` tuple exactly specifies a cell in HBase. It's possible to have an unbounded
                                             number of cells where the row and column are the same but the cell address differs only in its version dimension.

``min_versions``            ``0``            The minimum number of row versions to keep is configured per column family

``compression``             ``NONE``         HBase supports several different compression algorithms which can be enabled on a ColumnFamily.
                                             Valid values are ``NONE``(default), ``SNAPPY``, ``LZO``, ``LZ4``, or ``GZ``.

``ttl``                     ``FOREVER``      ColumnFamilies can set a TTL length in seconds, and HBase will automatically delete rows once the expiration
                                             time is reached.
=========================== ================ ==============================================================================================================
