# Zend Framework 1 Doctrine DBAL Adapter for MySql

A ZF1 Zend_Db adapter to help migration to Doctrine DBAL on legacy ZF1 projects.

## How to use

```php
$connection = $container->get(\Doctrine\DBAL\Connection::class); // Get your DBAL connection
$adapter = new Facile\ZF1DbAdapter\Mysql\MysqlDBALAdapter($connection);

Zend_Db_Table::setDefaultAdapter($adapter);
```
