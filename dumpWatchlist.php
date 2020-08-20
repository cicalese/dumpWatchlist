<?php
require_once __DIR__ . '/Maintenance.php';

class DumpWatchlist extends Maintenance {

	private const DEFAULT_BATCH_SIZE = 100000;

	public function __construct() {
		parent::__construct();
		$this->setBatchSize( self::DEFAULT_BATCH_SIZE );
		$this->addDescription( 'Dump watchlist.' );
		$this->addArg( 'outputFilename', 'Name of the output file', true );
	}

	public function execute() {

		$start_time = microtime(true);

		$prevTmpFileName = null;
		$startId = 1;
		$dbr = $this->getDB( DB_REPLICA, 'dump' );
		$maxId = $dbr->selectField( 'watchlist', 'MAX(wl_id)' );
		while ( $startId <= $maxId ) {
			$res = $dbr->select(
				'watchlist',
				[
					'count' => 'COUNT(*)',
					'wl_namespace',
					'wl_title'
				],
				[
					'wl_id >= ' . $startId,
					'wl_id < ' . ( $startId + $this->getBatchSize() )
				],
				__METHOD__,
				[
					'GROUP BY' => [ 'wl_namespace', 'wl_title' ]
				]
			);

			$length = $res->numRows();
			if ( $length < 1 ) {
				break;
			}

			$tmpFileName = tempnam( wfTempDir(), '' );
			$tmpFile = fopen( $tmpFileName, 'w' );

			if ( $prevTmpFileName ) {
				$prevTmpFile = fopen( $prevTmpFileName, 'r' );

				$index = 0;
				$row = $res->fetchRow();
				$count = $namespace = $title = null;
				$valid = $this->readline( $prevTmpFile, $count, $namespace, $title );
				while ( $index < $length && $valid ) {

					if ( $namespace < $row['wl_namespace'] ||
						( $namespace == $row['wl_namespace'] && $title < $row['wl_title'] ) ) {

						fwrite( $tmpFile, $count . "\t" . $namespace . "\t" .
							$title . PHP_EOL );
						$valid = $this->readline( $prevTmpFile, $count, $namespace, $title );

					} elseif ( $namespace == $row['wl_namespace'] && $title == $row['wl_title'] ) {

						fwrite( $tmpFile, ( intval( $count ) + intval( $row['count'] ) ) .
							"\t" . $namespace . "\t" . $title . PHP_EOL );
						$valid = $this->readline( $prevTmpFile, $count, $namespace, $title );
						$index++;
						$row = $res->fetchRow();

					} else if ( $this->titleExists( $row['wl_namespace'], $row['wl_title'] ) ) {

						fwrite( $tmpFile, $row['count'] . "\t" . $row['wl_namespace'] . "\t" .
							$row['wl_title'] . PHP_EOL );
						$index++;
						$row = $res->fetchRow();

					} else {

						$index++;
						$row = $res->fetchRow();

					}
				}

				while ( $index < $length ) {
					fwrite( $tmpFile, $row['count'] . "\t" .
						$row['wl_namespace'] . "\t" .
						$row['wl_title'] . PHP_EOL );
					$index++;
					$row = $res->fetchRow();
				}

				while ( $valid ) {
					fwrite( $tmpFile, $count . "\t" . $namespace . "\t" .
						$title . PHP_EOL );
					$valid = $this->readline( $prevTmpFile, $count, $namespace, $title );
				}

				fclose( $prevTmpFile );
				unlink( $prevTmpFileName );

			} else {
				foreach ( $res as $row ) {
					fwrite( $tmpFile, $row->count . "\t" . $row->wl_namespace . "\t" .
						$row->wl_title . PHP_EOL );
				}
			}

			fclose( $tmpFile );
			$prevTmpFileName = $tmpFileName;
			$startId = $startId + $this->getBatchSize();
		}

		if ( $prevTmpFileName ) {
			rename( $prevTmpFileName, $this->getArg(0 ) );
		}
		$end_time = microtime(true);
		$execution_time = ($end_time - $start_time);
		echo " Execution time of script = " . $execution_time . " sec." . PHP_EOL;
	}

	private function readline( $file, &$count, &$namespace, &$title ) {
		$line = fgets( $file );
		if ( $line === false ) {
			if ( feof( $file ) ) {
				return false;
			}
			$this->fatalError( "Error reading from intermediate file, exiting" );
		}
		$values = array_map( 'trim', explode( "\t", $line, 3 ) );
		if ( count( $values ) < 3 ) {
			$this->fatalError( "Invalid intermediate file, exiting" );
		}
		$count = $values[ 0 ];
		$namespace = $values[ 1 ];
		$title = $values[ 2 ];
		return true;
	}

	private $missingTitles = [];

	private function titleExists( $namespace, $title ) {
		if ( array_key_exists( $namespace . ':' . $title, $this->missingTitles ) ) {
			return false;
		}
		$t = Title::makeTitle( $namespace, $title  );
		if ( $t && $t->exists() ) {
			return true;
		}
		$this->missingTitles[$namespace . ':' . $title] = true;
		return false;
	}
}

$maintClass = DumpWatchlist::class;

require_once RUN_MAINTENANCE_IF_MAIN;
