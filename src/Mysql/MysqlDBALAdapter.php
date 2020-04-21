<?php

namespace Facile\ZF1DbAdapter\Mysql;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ConnectionException;
use PDO;
use PDOException;
use PDOStatement;
use Zend_Db;
use Zend_Db_Adapter_Exception;
use Zend_Db_Select;
use Zend_Db_Statement;
use Zend_Db_Adapter_Abstract;

class MysqlDBALAdapter extends Zend_Db_Adapter_Abstract
{
    /**
     * @var Connection
     */
    protected $dbal;

    /**
     * Default class name for a DB statement.
     *
     * @var string
     */
    protected $_defaultStmtClass = MysqlDBALStatement::class;

    /**
     * Keys are UPPERCASE SQL datatypes or the constants
     * Zend_Db::INT_TYPE, Zend_Db::BIGINT_TYPE, or Zend_Db::FLOAT_TYPE.
     *
     * Values are:
     * 0 = 32-bit integer
     * 1 = 64-bit integer
     * 2 = float or decimal
     *
     * @var array associative array of datatypes to values 0, 1, or 2
     */
    protected $_numericDataTypes = [
        Zend_Db::INT_TYPE => Zend_Db::INT_TYPE,
        Zend_Db::BIGINT_TYPE => Zend_Db::BIGINT_TYPE,
        Zend_Db::FLOAT_TYPE => Zend_Db::FLOAT_TYPE,
        'INT' => Zend_Db::INT_TYPE,
        'INTEGER' => Zend_Db::INT_TYPE,
        'MEDIUMINT' => Zend_Db::INT_TYPE,
        'SMALLINT' => Zend_Db::INT_TYPE,
        'TINYINT' => Zend_Db::INT_TYPE,
        'BIGINT' => Zend_Db::BIGINT_TYPE,
        'SERIAL' => Zend_Db::BIGINT_TYPE,
        'DEC' => Zend_Db::FLOAT_TYPE,
        'DECIMAL' => Zend_Db::FLOAT_TYPE,
        'DOUBLE' => Zend_Db::FLOAT_TYPE,
        'DOUBLE PRECISION' => Zend_Db::FLOAT_TYPE,
        'FIXED' => Zend_Db::FLOAT_TYPE,
        'FLOAT' => Zend_Db::FLOAT_TYPE,
    ];

    /**
     * Zf1DbDoctrineAdapter constructor.
     *
     * @param Connection $connection
     *
     * @throws \Zend_Db_Adapter_Exception
     */
    public function __construct(Connection $connection)
    {
        $options = [
            'dbname' => $connection->getDatabase(),
            'options' => [
                'allowSerialization' => false,
                'autoReconnectOnUnserialize' => false,
            ],
        ];

        $this->setDbal($connection);
        parent::__construct($options);
    }

    /**
     * Check for config options that are mandatory.
     * Throw exceptions if any are missing.
     *
     * @param array $config
     *
     * @throws Zend_Db_Adapter_Exception
     */
    protected function _checkRequiredOptions(array $config): void
    {
    }

    /**
     * Test if a connection is active
     *
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->getDbal()->isConnected();
    }

    /**
     * @return Connection
     */
    public function getDbal(): Connection
    {
        return $this->dbal;
    }

    /**
     * @param Connection $dbal
     *
     * @return $this
     */
    public function setDbal(Connection $dbal)
    {
        $this->dbal = $dbal;

        return $this;
    }

    /**
     * Force the connection to close.
     *
     * @return void
     */
    public function closeConnection(): void
    {
        $this->getDbal()->close();
    }

    /**
     * Leave autocommit mode and begin a transaction.
     *
     * @return $this
     */
    protected function _beginTransaction()
    {
        $this->_connect();
        $this->getDbal()->beginTransaction();

        return $this;
    }

    /**
     * Commit a transaction and return to autocommit mode.
     *
     * @throws ConnectionException
     *
     * @return $this
     */
    protected function _commit()
    {
        $this->_connect();
        $this->getDbal()->commit();

        return $this;
    }

    /**
     * Roll back a transaction and return to autocommit mode.
     *
     * @throws ConnectionException
     *
     * @return $this
     */
    protected function _rollBack()
    {
        $this->_connect();
        $this->getDbal()->rollBack();

        return $this;
    }

    /**
     * @return string
     */
    public function getQuoteIdentifierSymbol(): string
    {
        return $this->getDbal()->getDatabasePlatform()->getIdentifierQuoteCharacter();
    }

    /**
     * Returns a list of the tables in the database.
     *
     * @return array
     */
    public function listTables()
    {
        return $this->fetchCol('SHOW TABLES');
    }

    /**
     * Returns the column descriptions for a table.
     *
     * The return value is an associative array keyed by the column name,
     * as returned by the RDBMS.
     *
     * The value of each array element is an associative array
     * with the following keys:
     *
     * SCHEMA_NAME => string; name of database or schema
     * TABLE_NAME  => string;
     * COLUMN_NAME => string; column name
     * COLUMN_POSITION => number; ordinal position of column in table
     * DATA_TYPE   => string; SQL datatype name of column
     * DEFAULT     => string; default expression of column, null if none
     * NULLABLE    => boolean; true if column can have nulls
     * LENGTH      => number; length of CHAR/VARCHAR
     * SCALE       => number; scale of NUMERIC/DECIMAL
     * PRECISION   => number; precision of NUMERIC/DECIMAL
     * UNSIGNED    => boolean; unsigned property of an integer type
     * PRIMARY     => boolean; true if column is part of the primary key
     * PRIMARY_POSITION => integer; position of column in primary key
     *
     * @param string $tableName
     * @param string $schemaName OPTIONAL
     *
     * @return array
     */
    public function describeTable($tableName, $schemaName = null)
    {
        // @todo  use INFORMATION_SCHEMA someday when MySQL's
        // implementation has reasonably good performance and
        // the version with this improvement is in wide use.

        if ($schemaName) {
            $sql = 'DESCRIBE ' . $this->quoteIdentifier("$schemaName.$tableName", true);
        } else {
            $sql = 'DESCRIBE ' . $this->quoteIdentifier($tableName, true);
        }
        $stmt = $this->query($sql);

        // Use FETCH_NUM so we are not dependent on the CASE attribute of the PDO connection
        $result = $stmt->fetchAll(\Zend_Db::FETCH_NUM);

        $field = 0;
        $type = 1;
        $null = 2;
        $key = 3;
        $default = 4;
        $extra = 5;

        $desc = [];
        $i = 1;
        $p = 1;
        foreach ($result as $row) {
            [$length, $scale, $precision, $unsigned, $primary, $primaryPosition, $identity]
                = [null, null, null, null, false, null, false];
            if (\preg_match('/unsigned/', $row[$type])) {
                $unsigned = true;
            }
            if (\preg_match('/^((?:var)?char)\((\d+)\)/', $row[$type], $matches)) {
                $row[$type] = $matches[1];
                $length = $matches[2];
            } elseif (\preg_match('/^decimal\((\d+),(\d+)\)/', $row[$type], $matches)) {
                $row[$type] = 'decimal';
                $precision = $matches[1];
                $scale = $matches[2];
            } elseif (\preg_match('/^float\((\d+),(\d+)\)/', $row[$type], $matches)) {
                $row[$type] = 'float';
                $precision = $matches[1];
                $scale = $matches[2];
            } elseif (\preg_match('/^((?:big|medium|small|tiny)?int)\((\d+)\)/', $row[$type], $matches)) {
                $row[$type] = $matches[1];
                // The optional argument of a MySQL int type is not precision
                // or length; it is only a hint for display width.
            }
            if (\strtoupper($row[$key]) == 'PRI') {
                $primary = true;
                $primaryPosition = $p;
                if ($row[$extra] == 'auto_increment') {
                    $identity = true;
                } else {
                    $identity = false;
                }
                ++$p;
            }
            $desc[$this->foldCase($row[$field])] = [
                'SCHEMA_NAME' => null, // @todo
                'TABLE_NAME' => $this->foldCase($tableName),
                'COLUMN_NAME' => $this->foldCase($row[$field]),
                'COLUMN_POSITION' => $i,
                'DATA_TYPE' => $row[$type],
                'DEFAULT' => $row[$default],
                'NULLABLE' => (bool) ($row[$null] == 'YES'),
                'LENGTH' => $length,
                'SCALE' => $scale,
                'PRECISION' => $precision,
                'UNSIGNED' => $unsigned,
                'PRIMARY' => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY' => $identity,
            ];
            ++$i;
        }

        return $desc;
    }

    /**
     * Creates a connection to the database.
     *
     * @return void
     */
    protected function _connect(): void
    {
        $this->getDbal()->connect();
    }

    /**
     * Prepare a statement and return a PDOStatement-like object.
     *
     * @param string|Zend_Db_Select $sql SQL query
     *
     * @return Zend_Db_Statement|PDOStatement
     */
    public function prepare($sql)
    {
        $this->_connect();
        $stmt = new MysqlDBALStatement($this, $sql);
        $stmt->setFetchMode($this->_fetchMode);

        return $stmt;
    }

    /**
     * Gets the last ID generated automatically by an IDENTITY/AUTOINCREMENT column.
     *
     * As a convention, on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2), this method forms the name of a sequence
     * from the arguments and returns the last id generated by that sequence.
     * On RDBMS brands that support IDENTITY/AUTOINCREMENT columns, this method
     * returns the last value generated for such a column, and the table name
     * argument is disregarded.
     *
     * @param string $tableName  OPTIONAL Name of table
     * @param string $primaryKey OPTIONAL Name of primary key column
     *
     * @return string
     */
    public function lastInsertId($tableName = null, $primaryKey = null)
    {
        $this->_connect();

        return $this->getDbal()->lastInsertId();
    }

    /**
     * Set the fetch mode.
     *
     * @param int $mode
     *
     * @throws Zend_Db_Adapter_Exception
     *
     * @return void
     */
    public function setFetchMode($mode): void
    {
        switch ($mode) {
            case PDO::FETCH_LAZY:
            case PDO::FETCH_ASSOC:
            case PDO::FETCH_NUM:
            case PDO::FETCH_BOTH:
            case PDO::FETCH_NAMED:
            case PDO::FETCH_OBJ:
                $this->_fetchMode = $mode;
                break;
            default:
                throw new Zend_Db_Adapter_Exception("Invalid fetch mode '$mode' specified");
                break;
        }
    }

    /**
     * Adds an adapter-specific LIMIT clause to the SELECT statement.
     *
     * @param mixed   $sql
     * @param int $count
     * @param int $offset
     *
     * @throws Zend_Db_Adapter_Exception
     *
     * @return string
     */
    public function limit($sql, $count, $offset = 0)
    {
        $count = (int) $count;
        if ($count <= 0) {
            throw new Zend_Db_Adapter_Exception("LIMIT argument count=$count is not valid");
        }

        $offset = (int) $offset;
        if ($offset < 0) {
            throw new Zend_Db_Adapter_Exception("LIMIT argument offset=$offset is not valid");
        }

        $sql .= " LIMIT $count";
        if ($offset > 0) {
            $sql .= " OFFSET $offset";
        }

        return $sql;
    }

    /**
     * Check if the adapter supports real SQL parameters.
     *
     * @param string $type 'positional' or 'named'
     *
     * @return bool
     */
    public function supportsParameters($type)
    {
        switch ($type) {
            case 'positional':
            case 'named':
            default:
                return true;
        }
    }

    /**
     * Retrieve server version in PHP style
     *
     * @return string|null
     */
    public function getServerVersion()
    {
        $this->_connect();

        $wrappedConnection = $this->getDbal()->getWrappedConnection();

        if (! $wrappedConnection instanceof PDO) {
            throw new \LogicException('Invalid PDO connection');
        }

        try {
            $version = $wrappedConnection->getAttribute(PDO::ATTR_SERVER_VERSION);
        } catch (PDOException $e) {
            // In case of the driver doesn't support getting attributes
            return null;
        }
        $matches = null;
        if (\preg_match('/((?:\d{1,2}\.){1,3}\d{1,2})/', $version, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Quote a raw string.
     *
     * @param mixed $value     Raw string
     *
     * @return mixed           Quoted string
     */
    protected function _quote($value)
    {
        if (\is_int($value) || \is_float($value)) {
            return $value;
        }
        $this->_connect();

        return $this->getDbal()->quote($value);
    }

    /**
     * @param string|Zend_Db_Select $sql
     * @param mixed $bind
     *
     * @return array
     */
    protected function prepareSql($sql, $bind = [])
    {
        // is the $sql a Zend_Db_Select object?
        if ($sql instanceof Zend_Db_Select) {
            if (empty($bind)) {
                $bind = $sql->getBind();
            }

            $sql = $sql->assemble();
        }

        // make sure $bind to an array;
        // don't use (array) typecasting because
        // because $bind may be a Zend_Db_Expr object
        if (! \is_array($bind)) {
            $bind = [$bind];
        }

        return [$sql, $bind];
    }
}
