<?php

namespace Facile\ZF1DbAdapter\Mysql;

use Facile\ZF1DbAdapter\Zf1DbDBALAdapter;
use PDO;
use Zend_Db_Statement_Exception;

class MysqlDBALStatement extends \Zend_Db_Statement implements \IteratorAggregate
{
    /**
     * @var int
     */
    protected $_fetchMode = PDO::FETCH_ASSOC;

    /**
     * @var \Doctrine\DBAL\Driver\Statement
     */
    protected $_stmt;

    /**
     * Internal method called by abstract statment constructor to setup
     * the driver level statement
     *
     * @return void
     */
    protected function _prepare($sql): void
    {
        if ($sql instanceof \Zend_Db_Select) {
            $sql = $sql->assemble();
        }
        /** @var Zf1DbDBALAdapter $adapter */
        $adapter = $this->getAdapter();
        $this->_stmt = $adapter->getDbal()->prepare($sql);
    }

    /**
     * Executes a prepared statement.
     *
     * @param array $params OPTIONAL Values to bind to parameter placeholders
     *
     * @return bool
     */
    public function _execute(array $params = null)
    {
        return $this->_stmt->execute($params);
    }

    /**
     * Closes the cursor, allowing the statement to be executed again.
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return bool
     */
    public function closeCursor()
    {
        return $this->_stmt->closeCursor();
    }

    /**
     * Returns the number of columns in the result set.
     * Returns null if the statement has no result set metadata.
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return int the number of columns
     */
    public function columnCount()
    {
        return $this->_stmt->columnCount();
    }

    /**
     * Retrieves the error code, if any, associated with the last operation on
     * the statement handle.
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return string error code
     */
    public function errorCode()
    {
        return $this->_stmt->errorCode();
    }

    /**
     * Retrieves an array of error information, if any, associated with the
     * last operation on the statement handle.
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return array
     */
    public function errorInfo()
    {
        return $this->_stmt->errorInfo();
    }

    /**
     * Fetches a row from the result set.
     *
     * @param int $style  OPTIONAL Fetch mode for this fetch operation
     * @param int $cursor OPTIONAL Absolute, relative, or other
     * @param int $offset OPTIONAL Number for absolute or relative cursors
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return mixed array, object, or scalar depending on fetch mode
     */
    public function fetch($style = null, $cursor = null, $offset = null)
    {
        if (null !== $cursor) {
            throw new \RuntimeException('Cursor parameters provided, but not supported');
        }
        if (null !== $offset) {
            throw new \RuntimeException('Offset parameters provided, but not supported');
        }
        if ($style === null) {
            $style = $this->_fetchMode;
        }

        return $this->_stmt->fetch($style, $cursor ?: \PDO::FETCH_ORI_NEXT, $offset ?: 0);
    }

    /**
     * Retrieves the next rowset (result set) for a SQL statement that has
     * multiple result sets.  An example is a stored procedure that returns
     * the results of multiple queries.
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return bool
     */
    public function nextRowset()
    {
        throw new \Zend_Db_Statement_Exception(__FUNCTION__ . '() is not implemented');
    }

    /**
     * Returns the number of rows affected by the execution of the
     * last INSERT, DELETE, or UPDATE statement executed by this
     * statement object.
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return int     the number of rows affected
     */
    public function rowCount()
    {
        return $this->_stmt->rowCount();
    }

    /**
     * Required by IteratorAggregate interface
     *
     * @return \IteratorIterator
     */
    public function getIterator()
    {
        return new \IteratorIterator($this->_stmt);
    }

    /**
     * Bind a column of the statement result set to a PHP variable.
     *
     * @param string $column name the column in the result set, either by
     *                       position or by name
     * @param mixed  $param  reference to the PHP variable containing the value
     * @param mixed  $type   OPTIONAL
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return bool
     */
    public function bindColumn($column, &$param, $type = null)
    {
        try {
            return $this->_stmt->bindParam($column, $param, $type);
        } catch (\PDOException $e) {
            throw new Zend_Db_Statement_Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Binds a parameter to the specified variable name.
     *
     * @param mixed $parameter name the parameter, either integer or string
     * @param mixed $variable  reference to PHP variable containing the value
     * @param mixed $type      OPTIONAL Datatype of SQL parameter
     * @param mixed $length    OPTIONAL Length of SQL parameter
     * @param mixed $options   OPTIONAL Other options
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return bool
     */
    protected function _bindParam($parameter, &$variable, $type = null, $length = null, $options = null)
    {
        if (null !== $options) {
            throw new \RuntimeException('Options parameters provided, but not supported');
        }

        try {
            if ($type === null) {
                if (\is_bool($variable)) {
                    $type = PDO::PARAM_BOOL;
                } elseif ($variable === null) {
                    $type = PDO::PARAM_NULL;
                } elseif (\is_int($variable)) {
                    $type = PDO::PARAM_INT;
                } else {
                    $type = PDO::PARAM_STR;
                }
            }

            return $this->_stmt->bindParam($parameter, $variable, $type, $length);
        } catch (\PDOException $e) {
            throw new Zend_Db_Statement_Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Binds a value to a parameter.
     *
     * @param mixed $parameter name the parameter, either integer or string
     * @param mixed $value     scalar value to bind to the parameter
     * @param mixed $type      OPTIONAL Datatype of the parameter
     *
     * @throws Zend_Db_Statement_Exception
     *
     * @return bool
     */
    public function bindValue($parameter, $value, $type = null)
    {
        if (\is_string($parameter) && $parameter[0] != ':') {
            $parameter = ":$parameter";
        }

        $this->_bindParam[$parameter] = $value;

        try {
            return $this->_stmt->bindValue($parameter, $value, $type);
        } catch (\PDOException $e) {
            throw new Zend_Db_Statement_Exception($e->getMessage(), $e->getCode(), $e);
        }
    }
}
