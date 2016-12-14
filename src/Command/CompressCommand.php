<?php

namespace DbTools\Command;

use RuntimeException;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use DbTools\Utils;
use PDO;

class CompressCommand extends Command
{
    protected $inventory;

    public function configure()
    {
        $this->setName('compress')
            ->setDescription('Update all tables on a server to Barracuda compressed row_format')
            ->addArgument(
                'url',
                InputArgument::REQUIRED,
                'Connection URL to the server'
            )
        ;
    }

    public function execute(InputInterface $input, OutputInterface $output)
    {
        $url = $input->getArgument('url');
        $output->writeLn("<comment>URL:</comment>" . $url);
        $pdo = Utils::getServerPdo($url);
        
        $stmt = $pdo->prepare(
            "SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, ROW_FORMAT FROM information_schema.TABLES WHERE ROW_FORMAT='Compact' AND TABLE_SCHEMA !='mysql'"
        );
        $res = $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $curDbName = null;
        foreach ($rows as $row) {
            $dbName = $row['TABLE_SCHEMA'];
            $tableName = $row['TABLE_NAME'];
            
            if ($dbName != $curDbName) {
                echo $dbName . "\n";
                $stmt = $pdo->prepare(
                    "USE " . $dbName
                );
                $res = $stmt->execute();
                $curDbName = $dbName;
            }
            
            echo "  Compressing $dbName.$tableName\n";
            $stmt = $pdo->prepare(
                "ALTER TABLE " . $tableName . " ROW_FORMAT=compressed;"
            );
            $res = $stmt->execute();
        }
        //print_r($rows);
    }
}
