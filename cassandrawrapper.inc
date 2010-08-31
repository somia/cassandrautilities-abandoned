<?php
/**
 * Copyright (c) 2008 Sulake Dynamoid Oy All rights reserved.:
 *
 *  Redistribution and use in source and binary forms, with or without
 *	modification, are permitted provided that the following conditions are met:
 *
 *	- Redistributions of source code must retain the above copyright notice, this
 *	  list of conditions and the following disclaimer.
 *
 *	- Redistributions in binary form must reproduce the above copyright notice,
 *	  this list of conditions and the following disclaimer in the documentation
 *	  and/or other materials provided with the distribution.
 *
 *	- Neither the name of the 'igbinary' nor the names of its contributors may
 *	  be used to endorse or promote products derived from this software without
 *	  specific prior written permission.
 *
 *	THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 *	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *	THE POSSIBILITY OF SUCH DAMAGE.
 *
 *
 * Example how to use CassandraWrapper class with Singleton pattern

   class Cassandra extends CassandraWrapper {
	static private $instance = false;

	public static function get_instance() {

		if (self::$instance === false) {
			global $installation;

			self::$instance = new CassandraWrapper(
						$installation['cassandra']['keyspace'],
						$installation['cassandra']['nodes'],
						Installation::get_site() . '_',
						null
					);
		}

		return self::$instance;
	}
   }
 *
 *
 * Usage:
    $cassandra = Cassandra::get_instance();

  	$column_path = new cassandra_ColumnPath();
	$column_path->column_family = 'BlogEntries';
	$column_path->super_column = Cassandra::pack_longtype($long_for_column_name);
	$column_path->column = 'content';

	$column_struct = $cassandra->get_column($row_key, $column_path);
 *
 */




/**
 * Logger class for CassandraWrapper class. Extend this class and provide your own functions
 * to log your performance and debug data anywhere you want.
 */
class CassandraLogger {

	// Cassandra operation result types
	const CASSANDRA_OPERATION_STATUS_OK = 1;
	const CASSANDRA_OPERATION_STATUS_CLUSTER_OFFLINE = 2;
	const CASSANDRA_OPERATION_STATUS_OUT_OF_RETRIES = 3;
	const CASSANDRA_OPERATION_STATUS_CONNECTION_FAILED = 4;

	// Cassandra operation types
	const CASSANDRA_OPERATION_TYPE_INSERT = 1;
	const CASSANDRA_OPERATION_TYPE_DELETE = 2;
	const CASSANDRA_OPERATION_TYPE_BATCH_INSERT = 3;
	const CASSANDRA_OPERATION_TYPE_GET_COLUMN = 4;
	const CASSANDRA_OPERATION_TYPE_GET_SLICE = 5;
	const CASSANDRA_OPERATION_TYPE_CONNECT = 6;


	/**
	 * Fired when entire cluster is down or we couldn't connect to any node.
	 * @param  $message
	 * @return void
	 */
	public function fatalConnectionError($message) {}

	/**
	 * Fired when we are retrying a connection
	 * @param  $old_host Old host which we tried to connect before but failed
	 * @param  $new_host New host where we are trying to connect
	 * @param  $message Message with additional details
	 * @param  $attempt_number Connection attempt number
	 * @return void
	 */
	public function retryConnection($old_host, $new_host, $message, $attempt_number) {}

	/**
	 * Fired when a single connection attempt failed
	 * @param  $hostport string "host:port" where we tried to connect
	 * @param  $exception Exception Related exception
	 * @param  $time_elapsed Elapsed time in microseconds
	 * @param  $attempt_number Attempt number
	 * @return void
	 */
	public function connectionAttemptError($hostport, $exception, $time_elapsed, $attempt_number) {}

	/**
	 * Logs a batch insert operation.
	 * @param  $type int  One of CASSANDRA_OPERATION_TYPE_* const
	 * @param  $time_elapsed int Elapsed time in microseconds
	 * @param  $tries int Number of tries operation took
	 * @param  $success int One of CASSANDRA_OPERATION_STATUS_* const
	 * @param  $exception Exception Last exception
	 * @return void
	 */
	public function logOperation($type, $time_elapsed, $tries, $status, $exception) {}

	/**
	 * In case cassandra_InvalidRequestException was catched
	 * @param  $type Type of request (like get_slice, get_column etc)
	 * @param  $exception cassandra_InvalidRequestException exception instance
	 * @param  $key Key which was used or null if this was a batch operation
	 * @return void
	 */
	public function invalidRequest($type, $exception, $key) {}
}

/**
 * Cassandra thrift api wrapper.
 *
 * This class encapsulates all thrift api calls and adds good failover, exception handling, backpressure
 * handling and node failure logic to make the api as fault tolerant as possible. It also provides
 * easy and extendable way to log data cassandra operations. This includes both performance tracking
 * and error logging.
 *
 * As all cassandra requests are repeatable and stateless the api can try the request again to another node
 * to maximise change of success. Cassandra might also throttle requests if there's too much backpressure. This
 * results in failed connections which can be just retried into another node.
 *
 * @throws #Ccassandra_InvalidRequestException|#Ccassandra_NotFoundException|#CException|?
 *
 */
class CassandraWrapper {



	/**
	 * How many times we try to retry connection
	 */
	private $connection_retries = 5;

	/**
	 * How many times we try to retry operation
	 */
	private $operation_retries = 6;

	/**
	 * Tells if thrift should use TBinaryProtocolAccelerated instead of the default TBinaryProtocol
	 * @var bool
	 */
	static private $accelerated = false;

	/**
	 * Tells if thrift should use TBufferedTransport instead of TFramedTransport
	 * @var bool
	 */
	static private $buffered_mode = false;

	/** @var array All cassandra nodes as "host:port" strings from installation config */
	protected $all_nodes = array();

	/** @var array All usable (not killed) nodes as "host:port" strings */
	protected $usable_nodes = array();

	// Ready-to-use arrays for hosts and ports which can be passed to thrift connect function.
	private $hosts = array();
	private $ports = array();

	/**
	 * Additional debug information about last operation.
	 */
	private $last_operation = null;

	/**
	 * Key prefix which is used to prefix every key.
	 */
	private $key_prefix = '';

	/**
	 * Instance of Thrift TSocketPool
	 */
	private $socket = null;

	/**
	 * Thrift client instance. Should be usable after constructor has been called.
	 */
	private $client = null;

	/**
	 * Instance of CassandraLogger or a class which extends it.
	 * @var CassandraLogger
	 */
	private $logger;

	/*
	 * Keyspace to use
	 */
	private $keyspace = null;

	/**
	 * Creates new instance.
	 *
	 * @param $keyspace Keyspace to use
	 * @param $nodelist array Array of nodes. Each array item is a "host:port" pair eg array('node1:9160', 'node2:9160');
	 * @param CassandraLogger $logger Instance of class implementing CassandraLogger which can be used to log cassandra performance, errors and other data
	 */
	public function __construct($keyspace, array $nodelist, $key_prefix = null, CassandraLogger $logger = null) {

		$this->keyspace = $keyspace;

		$this->all_nodes = $nodelist;
		shuffle($this->all_nodes);
		$this->usable_nodes = $this->all_nodes;

		if ($logger) {
			$this->logger = $logger;
		} else {
			$this->logger = new CassandraLogger();
		}

		if ($key_prefix != null) {
			$this->key_prefix = $key_prefix;
		}

		try {
			$this->connect();
			return;
		} catch (Exception $e) {
			$this->last_exception = $e;
		}

		$this->logger->fatalConnectionError($this->last_exception);
		throw $this->last_exception;

	}

	/**
	 * Builds $this->hosts and $this->ports arrays which can be then used with thrift to connect to cassandra nodes
	 * from $this->usable_nodes list.
	 *
	 * @return void
	 */
	private function build_usable_hosts() {
		$this->hosts = array();
		$this->ports = array();

		// In case we have dropped all our nodes in previous failed attemps we just start all over again
		if (count($this->usable_nodes) == 0) {
			$this->usable_nodes = $this->all_nodes;
		}

		foreach ($this->usable_nodes as $key => $hostport) {
			list($host, $port) = explode(':', $hostport);
			$this->hosts[] = $host;
			$this->ports[] = $port;
		}

	}

	/**
	 * Kills a dead node by removing it from $this->usable_nodes array
	 * @return void
	 */
	private function kill_node($host_to_kill) {
		foreach ($this->usable_nodes as $key => $hostport) {
			list($host, $port) = explode(':', $hostport);
			if ($host == $host_to_kill) {

				$before = count($this->usable_nodes);
				unset($this->usable_nodes[$key]);
				$after = count($this->usable_nodes);
				//Logger::debug("Killing cassandra node $host_to_kill. size before: $before, after: $after");
				return;
			}
		}
	}

	protected function connect() {
		$start = microtime(true);
		$retry = false;
		$tries = 0;

		// Retry loop
		do {
			try {
				// Detect if this is a reconnection
				if ($this->transport) {
					$old_host = $this->socket->getHost();
					$this->kill_node($old_host);
					$this->transport->close();
					$this->transport = null;
					$retry = true;
				}

				// Populate $this->hosts and $this->ports arrays from $this->usable_nodes array
				$this->build_usable_hosts();

				$this->socket = new TSocketPool($this->hosts, $this->ports, false);
				$this->socket->setDebug(true);
				$this->socket->setRandomize(false);
				$this->socket->setSendTimeout(1000);
				$this->socket->setRecvTimeout(1000);
				$this->socket->setRetryInterval(15);
				$this->socket->setNumRetries(2);

				if (self::$buffered_mode) {
					$this->transport = new TBufferedTransport($this->socket, 1024, 1024);
				} else {
					$this->transport = new TFramedTransport($this->socket);
				}				

				if (self::$accelerated) {
					$this->protocol = new TBinaryProtocolAccelerated($this->transport);
				} else {
					$this->protocol = new TBinaryProtocol($this->transport);
				}
				$this->client = new CassandraClient($this->protocol);
				$this->transport->open();

				// Log some debugging if we are retrying an operation. Don't bother to log after single failure as it might just be backpressure
				if ($retry) {
					$msg = '';
					if ($this->last_operation) {
						$msg .= 'last operation: ' . $this->last_operation;
					}
					if ($this->last_exception != null) {
						$msg .= ', exception: ' . $this->last_exception->getMessage();
					}
					$this->logger->retryConnection($old_host, $this->get_connected_hostport(), $msg, $tries);

				}

				return;
			} catch (Exception $e) {
				$end = microtime(true);
				$tries++;

				$time_elapsed = (int) (($end - $start) * 1000000);
				$this->logger->connectionAttemptError($this->get_connected_hostport(), $e, $time_elapsed, $tries);
				$this->last_exception = $e;
			}
		} while ($tries < $this->connection_retries);
		$this->logger->fatalConnectionError($this->last_exception);
		throw $this->last_exception;

	}

	public function __destruct() {
		$this->close();
	}

	public function close() {
		if ($this->transport) {
			$this->transport->close();
		}
	}

	public function get_client() {
		return $this->client;
	}

	/**
	 * Makes batch insert to default keyspace with key
	 * @param $key key to update
	 * @param $batch_mutation array where key is (Super)ColumnFamily key and value is cassandra_ColumnOrSuperColumn
	 * @param $consistency_level defaults to cassandra_ConsistencyLevel::ONE
	 */
	public function batch_insert($key, $batch_mutation, $consistency_level = cassandra_ConsistencyLevel::QUORUM) {
		$tries = 0;
		$delay = 1000;
		$this->last_exception = null;
		$end = null;

		// Workaround thrift TBinaryProtocolAccelerated which would do settype($key, 'string') otherwise and thus fucking all up
		if (!is_string($key)) {
			$key = "$key";
		}

		$key = $this->key_prefix . $key;

		$this->last_operation = 'batch_insert';

		do {
			try {
				$start = microtime(true);
				$this->client->batch_insert($this->keyspace, $key, $batch_mutation, $consistency_level);
				$end = microtime(true);

				$this->logger->logBatchInsertOperation((int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OK);

				return;

			} catch (cassandra_InvalidRequestException $e) {
				$this->logger->invalidRequest('batch_insert', $e, $key);
				throw $e;
			} catch (Exception $e) {
				$this->last_exception = $e;
				$tries++;

				// sleep for some time and try again
				usleep($delay);
				$delay = $delay * 3;
				$this->connect();
				$tries++;
			}


		} while ($tries < $this->operation_retries);

		if ($end == null) {
			$end = microtime(true);
		}

		$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_BATCH_INSERT, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OUT_OF_RETRIES, $this->last_exception);


		throw $this->last_exception;

	}

	/**
	 * Makes batch insert to default keyspace with key
	 * @param $key key to update
	 * @param $batch_mutation array where key is (Super)ColumnFamily key and value is cassandra_ColumnOrSuperColumn
	 * @param $consistency_level defaults to cassandra_ConsistencyLevel::ONE
	 */
	public function batch_mutate($batch_mutation, $consistency_level = cassandra_ConsistencyLevel::QUORUM) {
		$tries = 0;
		$this->last_exception = null;
		$delay = 5000;
		$end = null;

		$fixed_mutation = array();
		if ($this->key_prefix != '') {
			foreach ($batch_mutation as $row_key => $row_value) {
				$fixed_mutation[$this->key_prefix . $row_key] = $row_value;
			}
		} else {
			$fixed_mutation = $batch_mutation;
		}

		$this->last_operation = 'batch_mutate';

		do {
			try {
				$start = microtime(true);
				$this->client->batch_mutate($this->keyspace, $fixed_mutation, $consistency_level);
				$end = microtime(true);

				$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_BATCH_INSERT, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OK, null);

				return;
			} catch (cassandra_InvalidRequestException $e) {
				$this->logger->invalidRequest($this->last_operation, $e, null);
				throw $e;
			} catch (Exception $e) {
				$this->last_exception = $e;
				$tries++;

				usleep($delay);
				$delay = $delay * 2;
				$this->connect();

			}


		} while ($tries < $this->operation_retries);

		if ($end == null) {
			$end = microtime(true);
		}
		$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_BATCH_INSERT, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OUT_OF_RETRIES, $this->last_exception);

		throw $this->last_exception;

	}

	/**
	 * Makes batch insert to default keyspace with key
	 * @param $key key to update
	 * @param $column_path what to update
	 * @param $value value to be set
	 * @param $timestamp defaults to Cassandra::get_microtimestamp()
	 * @param $consistency_level defaults to cassandra_ConsistencyLevel::ONE
	 */
	public function insert($key, cassandra_ColumnPath $column_path, $value, $timestamp = null, $consistency_level = cassandra_ConsistencyLevel::QUORUM) {
		$tries = 0;
		$this->last_exception = null;
		$delay = 1000;
		$end = null;

		if ($timestamp == null) {
			$timestamp = Cassandra::get_microtimestamp();
		}

		// Workaround thrift TBinaryProtocolAccelerated which would do settype($key, 'string') otherwise and thus fuck it all up
		if (!is_string($key)) {
			$key = "$key";
		}

		$key = $this->key_prefix . $key;

		$this->last_operation = 'insert, key: ' . $key . ', CF: ' . $column_path->column_family;

		do {
			try {
				$start = microtime(true);
				$this->client->insert($this->keyspace, $key, $column_path, $value, $timestamp, $consistency_level);
				$end = microtime(true);

				$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_INSERT, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OK, null);

				return;
			} catch (cassandra_InvalidRequestException $e) {
				$this->logger->invalidRequest($this->last_operation, $e, $key);
				throw $e;
			} catch (Exception $e) {
				$this->last_exception = $e;


				// sleep for some time and try again
				usleep($delay);
				$delay = $delay * 3;
				$this->connect();
				$tries++;
			}



		} while ($tries < $this->operation_retries);

		if ($end == null) {
			$end = microtime(true);
		}
		$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_INSERT, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OUT_OF_RETRIES, $this->last_exception);

		throw $this->last_exception;

	}

	/**
	 * Returns cassandra_Column for given key at given column_path
	 * @param $key key to get
	 * @param $column_path where to get
	 * @param $consistency_level defaults to cassandra_ConsistencyLevel::ONE
	 * @return cassandra_Column or null if key/column was not found
	 */
	public function get_column($key, cassandra_ColumnPath $column_path, $consistency_level = cassandra_ConsistencyLevel::QUORUM) {
		$tries = 0;
		$this->last_exception = null;
		$delay = 1000;
		$end = null;

		// Workaround thrift TBinaryProtocolAccelerated which would do settype($key, 'string') otherwise and thus fucking all up
		if (!is_string($key)) {
			$key = "$key";
		}

		$key = $this->key_prefix . $key;

		$this->last_operation = 'get_column, key: ' . $key . ', CF: ' . $column_path->column_family;

		do {

			try {
				$start = microtime(true);
				$c_or_sc = $this->client->get($this->keyspace, $key, $column_path, $consistency_level);
				$end = microtime(true);

				$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_GET_COLUMN, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OK, null);

				return $c_or_sc->column;

			} catch (cassandra_NotFoundException $e) {
				return null;

			} catch (cassandra_InvalidRequestException $e) {
				$this->logger->invalidRequest($this->last_operation, $e, $key);
				throw $e;

			} catch (Exception $e) {
				$this->last_exception = $e;

				// sleep for some time and try again
				usleep($delay);
				$delay = $delay * 3;
				$this->connect();
				$tries++;
			}

		} while ($tries < $this->operation_retries);


		if ($end == null) {
			$end = microtime(true);
		}
		$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_GET_COLUMN, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OUT_OF_RETRIES, $this->last_exception);

		throw $this->last_exception;

	}

	/**
	 * Get the group of columns contained by column_parent (either a ColumnFamily name
	 * or a ColumnFamily/SuperColumn name pair) specified by the given SlicePredicate struct.
	 * @param $key
	 * @param cassandra_ColumnParent $column_parent
	 * @param cassandra_SlicePredicate $predicate
	 * @param int $consistency_level
	 * @return array of cassandra_ColumnOrSuperColumn
	 */
	public function get_slice($key, cassandra_ColumnParent $column_parent, cassandra_SlicePredicate $predicate, $consistency_level = cassandra_ConsistencyLevel::QUORUM) {
		$tries = 0;
		$delay = 1000;
		$this->last_exception = null;

		// Workaround thrift TBinaryProtocolAccelerated which would do settype($key, 'string') otherwise and thus fucking all up
		if (!is_string($key)) {
			$key = "$key";
		}

		$key = $this->key_prefix . $key;

		$this->last_operation = 'get_slice, key: ' . $key . ', CF: ' . $column_parent->column_family;

		do {

			try {
				$start = microtime(true);
				$ret = $this->client->get_slice($this->keyspace, $key, $column_parent, $predicate, $consistency_level);
				$end = microtime(true);

				$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_GET_SLICE, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OK, null);

				return $ret;
			} catch (cassandra_NotFoundException $e) {
				return null;
			} catch (cassandra_InvalidRequestException $e) {
				$this->logger->invalidRequest($this->last_operation, $e, $key);
				throw $e;
			} catch (Exception $e) {
				$this->last_exception = $e;

				// sleep for some time and try again
				usleep($delay);
				$delay = $delay * 3;
				$this->connect();
				$tries++;
			}

		} while ($tries < $this->operation_retries);

		if ($end == null) {
			$end = microtime(true);
		}
		$this->logger->logOperation(CassandraLogger::CASSANDRA_OPERATION_TYPE_GET_SLICE, (int) (($end - $start) * 1000000), $tries, CassandraLogger::CASSANDRA_OPERATION_STATUS_OUT_OF_RETRIES, $this->last_exception);

		throw $this->last_exception;
	}

	/**
	 * Returns a cassandra_ColumnPath() with column_family, super_column and column set from the parameters.
	 * @return cassandra_ColumnPath
	 */
	public static function column_path($column_name, $column_family, $super_column = null) {
		$columnPath = new cassandra_ColumnPath();
		$columnPath->column_family = $column_family;
		$columnPath->super_column = $super_column;
		$columnPath->column = $column_name;

		return $columnPath;
	}

	/**
	 * Returns a cassandra_ColumnParent with column_family and super_column set from the parameters.
	 * @return cassandra_ColumnParent
	 */
	public static function column_parent($column_family, $super_column = null) {
		$columnParent = new cassandra_ColumnParent();
		$columnParent->column_family = $column_family;
		$columnParent->super_column = $super_column;

		return $columnParent;
	}

	/**
	 * Returns a cassandra_Column with name, value and timestamp set from the parameters.
	 * Timestamp is Cassandra::get_microtimestamp() if it's null.
	 * @return cassandra_Column
	 */
	public static function column($name, $value, $timestamp = null) {
		$column = new cassandra_Column();
		$column->name = $name;
		$column->value = $value;
		if ($timestamp == null) {
			$timestamp = Cassandra::get_microtimestamp();
		}
		$column->timestamp = $timestamp;

		return $column;
	}

	/**
	 * Returns a cassandra_SuperColumn with name and array of SuperColumns.
	 * @return cassandra_SuperColumn
	 */
	public static function super_column($name, $supercolumns = null) {
		$supercolumn = new cassandra_SuperColumn();
		$supercolumn->name = $name;
		if (is_array($supercolumns)) {
			$supercolumn->columns = $supercolumns;
		} else {
			$supercolumn->columns = array();
			$first = true;
			foreach (func_get_args() as $arg) {
				if ($first) {
					$first = false;
					continue;
				}
				$supercolumn->columns[] = $arg;
			}
		}

		return $supercolumn;
	}

	/**
	 * Takes n cassandra_Column as argument and builds an array of cassandra_ColumnOrSuperColumn with each items column
	 * attribute set to the n:th cassandra_Column argument.
	 * @return array of cassandra_ColumnOrSuperColumn
	 */
	public static function columnize() {
		$arr = array();
		foreach (func_get_args() as $arg) {
			$c_or_sc = new cassandra_ColumnOrSuperColumn();
			$c_or_sc->column = $arg;
			$arr[] = $c_or_sc;
		}

		return $arr;
	}

	/**
	 * Creates and returns a cassandra_ColumnOrSuperColumn which has column or super_column set to
	 * what has been supplied in the argument. The argument must be cassandra_Column or
	 * cassandra_SuperColumn.
	 * @static
	 * @param  $something
	 * @return cassandra_ColumnOrSuperColumn
	 */
	public static function column_or_super_column($something) {
		$column_or_super_column = new cassandra_ColumnOrSuperColumn();
		if ($something instanceof cassandra_Column) {
			$column_or_super_column->column = $something;
		} else if ($something instanceof cassandra_SuperColumn) {
			$column_or_super_column->super_column = $something;
		} else {
			throw new Exception("something wasn't column nor supercolumn. Cassandra request will fail!");
		}

		return $column_or_super_column;
	}

	/**
	 * Takes n cassandra_SuperColumn as argument and builds an array of cassandra_ColumnOrSuperColumn with each items column
	 * attribute set to the n:th cassandra_SuperColumn argument.
	 * @return array of cassandra_ColumnOrSuperColumn
	 */
	public static function super_columnize() {
		$arr = array();
		foreach (func_get_args() as $arg) {
			$c_or_sc = new cassandra_ColumnOrSuperColumn();
			$c_or_sc->super_column = $arg;
			$arr[] = $c_or_sc;
		}

		return $arr;
	}

	/**
	 * Returns a cassandra Mutation structure with ColumnOrSuperColumn or Deletion structure inside it.
	 * the parameter must be cassandra_ColumnOrSuperColumn or a cassandra_Deletion
	 * @static
	 * @param $something
	 * @return cassandra_Mutation
	 */
	public static function mutation($something) {
		$mutation = new cassandra_Mutation();

		if ($something instanceof cassandra_ColumnOrSuperColumn) {
			$mutation->column_or_supercolumn = $something;
		} else if ($something instanceof cassandra_Deletion) {
			$mutation->deletion = $something;
		} else {
			throw new Exception("something wasn't cassandra_ColumnOrSuperColumn nor cassandra_Deletion. Cassandra request will fail!");
		}

		return $mutation;
	}

	/**
	 * Returns a cassandra_SlicePredicate with column_names and slice_range set from the parameters.
	 * @return cassandra_SlicePredicate
	 */
	public static function slice_predicate($column_names = null, $slice_range = null) {
		$slicePredicate = new cassandra_SlicePredicate();
		$slicePredicate->column_names = $column_names;
		$slicePredicate->slice_range = $slice_range;

		return $slicePredicate;
	}

	/**
	 * Returns a cassandra_SliceRange with $start, $finish, $reversed and $count set from the parameters.
	 * @return cassandra_SliceRange
	 */
	public static function slice_range($start = '', $finish = '', $reversed = false, $count = 100) {
		$sliceRange = new cassandra_SliceRange();
		$sliceRange->start = $start;
		$sliceRange->finish = $finish;
		$sliceRange->reversed = $reversed;
		$sliceRange->count = $count;

		return $sliceRange;
	}

	/**
	 * Returns value for given key at given column_path
	 * @param $key key to get
	 * @param $column_path where to get
	 * @param $consistency_level defaults to cassandra_ConsistencyLevel::ONE
	 *
	 */
	public function get_column_value($key, cassandra_ColumnPath $column_path, $consistency_level = cassandra_ConsistencyLevel::QUORUM) {
		$c_or_sc = $this->client->get($this->keyspace, $key, $column_path, $consistency_level);
		return $c_or_sc->column->value;
	}


	/**
	 * Returns default keyspace.
	 * @return string keyspace name
	 */
	public function get_keyspace() {
		return $this->keyspace;
	}

	/**
	 * Returns 'host:port' string for current node where thrift currently has the connection.
	 * @return
	 */
	public function get_connected_hostport() {
		if ($this->socket) {
			return $this->socket->getHost() . ':' . $this->socket->getPort();
		} else {
			return null;
		}
	}

	/**
	 * Helper class for creating 64bit long timestamp in microseconds.
	 * @static
	 * @return float
	 */
	public static function get_microtimestamp() {
		return floor(microtime(true) * 1000000);
	}

	public static function useAccelerated() {
		self::$accelerated = true;
	}

	/**
	 * Turns the Thrift client to use TBufferedTransport instead of the default TFramedTransport
	 * @static
	 * @return void
	 */
	public static function useBufferedMode() {
		self::$buffered_mode = true;
	}

	/**
	 * Takes php integer and packs it to 64bit (8 bytes) long big endian binary representation.
	 *
	 * @param   integer  $x
	 * @return  string eight bytes long binary repersentation of the integer in big endian order.
	 */
	public static function pack_longtype($x) {
		return pack(
			'C8',
			($x >> 56) & 0xff,
			($x >> 48) & 0xff,
			($x >> 40) & 0xff,
			($x >> 32) & 0xff,
			($x >> 24) & 0xff,
			($x >> 16) & 0xff,
			($x >> 8) & 0xff,
			$x & 0xff
		);
	}

	/**
	 * Takes eight bytes long big endian binary representation of an integer and unpacks it to a php integer.
	 *
	 * @param   string  $x
	 * @return  integer
	 */
	public static function unpack_longtype($x) {
		$a = unpack('C8', $x);
		return ($a[1] << 56) + ($a[2] << 48) + ($a[3] << 40) + ($a[4] << 32) + ($a[5] << 24) + ($a[6] << 16) + ($a[7] << 8) + $a[8];
	}

}
