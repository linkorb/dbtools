<?php

namespace DbTools\Command;

use RuntimeException;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use DbTools\Utils;
use Symfony\Component\Process\Process;
use Symfony\Component\Process\Exception\ProcessFailedException;
use PDO;

class ServerCopyCommand extends Command
{
    public function configure()
    {
        $this->setName('server-copy')
            ->setDescription('Dump/import all databases from server1 to server2')
            ->addArgument(
                'url1',
                InputArgument::REQUIRED,
                'Connection URL to the server1'
            )
            ->addArgument(
                'url2',
                InputArgument::REQUIRED,
                'Connection URL to the server2'
            )
        ;
    }

    public function execute(InputInterface $input, OutputInterface $output)
    {
        $url1 = $input->getArgument('url1');
        $url2 = $input->getArgument('url2');
        $output->writeLn("<comment>URL1: </comment>" . $url1);
        $output->writeLn("<comment>URL2: </comment>" . $url2);
        $pdo1 = Utils::getServerPdo($url1);
        $pdo2 = Utils::getServerPdo($url2);

        $stmt = $pdo1->prepare(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA"
        );
        $res = $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $prefix = '';
        $postfix = '_a';
        foreach ($rows as $row) {
            $dbName = $row['SCHEMA_NAME'];
            if (substr($dbName, -2) !='_a') {
                echo "DB: $dbName\n";
                switch ($dbName) {
                    case 'information_schema':
                    case 'performance_schema':
                    case 'mysql':
                        echo " * Skip\n";
                        break;
                    default:
                        $newDbName = $prefix . $dbName . $postfix;
                        try {
                            echo "Creating $newDbName\n";
                            $stmt = $pdo2->prepare(
                                "CREATE DATABASE " . $newDbName
                            );
                            $stmt->execute();
                        } catch (\Exception $e) {
                            // ignore existing dbs
                        }
                            
                        $cmd = 'mysqldump -f -u ' . parse_url($url1, PHP_URL_USER);
                        $cmd .= ' -p' . parse_url($url1, PHP_URL_PASS);
                        $cmd .= ' -h ' . parse_url($url1, PHP_URL_HOST);
                        $cmd .= ' --single-transaction';
                        $cmd .= ' --triggers --opt --routines';
                        $cmd .= ' --master-data=2';
                        $cmd .= ' ' . $dbName;
                        
                        $cmd .= ' | mysql -u ' .  parse_url($url2, PHP_URL_USER);
                        $cmd .= ' -p' . parse_url($url2, PHP_URL_PASS);
                        $cmd .= ' -h ' . parse_url($url2, PHP_URL_HOST);
                        $cmd .= ' ' . $newDbName;
                        echo $cmd . "\n";
                        
                        $timeout = 60 * 60 * 2;
                        $process = new Process($cmd);
                        $process->setTimeout($timeout);
                        $process->setIdleTimeout($timeout);
                        $process->run();
                        if (!$process->isSuccessful()) {
                            throw new ProcessFailedException($process);
                        }

                        // dump/import
                        break;
                }
            }
        }
    }
}
