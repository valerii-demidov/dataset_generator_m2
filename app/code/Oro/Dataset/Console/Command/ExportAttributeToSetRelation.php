<?php
/**
 * @category   Oro
 * @package    Oro_Dataset
 * @copyright  Copyright (c) 2015 Oro Inc. DBA MageCore (http://www.magecore.com)
 */

namespace Oro\Dataset\Console\Command;

use Magento\CatalogUrlRewrite\Model\CategoryUrlPathGenerator;
use Magento\CatalogUrlRewrite\Model\ProductUrlPathGenerator;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\App\ObjectManagerFactory;
use Magento\Framework\Filesystem;
use Magento\Framework\ObjectManagerInterface;
use Magento\Store\Model\Store;
use Magento\TestFramework\Event\Magento;
use Symfony\Component\Config\Definition\Exception\Exception;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;


/**
 * Command for executing dataset generation
 */
class ExportAttributeToSetRelation extends Command
{
    const EXPORT_FILE_NAME = 'export_attribute_to_set_relation.csv';

    private $objectManager;
    private $resource;
    private $adapter;
    private $resourceConnection;
    private $eavConfig;
    private $catalogProductEntityTypeId;

    /**
     * ExportCategory constructor.
     * @param ObjectManagerInterface $objectManager
     */
    public function __construct(
        ObjectManagerInterface $objectManager,
        \Magento\Framework\App\ResourceConnection $resource,
        \Magento\Framework\App\ResourceConnection $resourceConnection,
        \Magento\Eav\Model\Config $eavConfig
    ) {
        $this->objectManager = $objectManager;
        $this->resource = $resource;
        $this->adapter = $this->resource->getConnection();
        $this->resourceConnection = $resourceConnection;

        $this->eavConfig = $eavConfig;
        $this->catalogProductEntityTypeId = $eavConfig->getEntityType('catalog_product')->getId();

        parent::__construct();
    }

    protected function configure()
    {
        $this->setName('exportattributetosetrelation:run')
            ->setDescription('Run Export Of Attribute To Set Relation');


        parent::configure();
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $relationsData = $this->prepareAttributeToSetCollection();
        $this->prepareCsv($relationsData);
    }

    /**
     * @return mixed
     */
    protected function prepareAttributeToSetCollection()
    {
        $connection = $this->resourceConnection->getConnection();
        $eavEntityAttributeTable = $this->getTable('eav_entity_attribute'); //eav_entity_attribute   - связи
        $eavAttributeSetTable = $this->getTable('eav_attribute_set'); //eav_attribute_set - аттрибу сет
        $eavAttributeable = $this->getTable('eav_attribute'); //eav_attribute - описание атрибута

        $selectAttributeToSet = $connection->select()
            ->from(array('eea' => $eavEntityAttributeTable),['attribute_id','attribute_set_id'])
            ->join(
                array('ea' => $eavAttributeable),
                'eea.attribute_id = ea.attribute_id',
                ['attribute_code', 'frontend_label as attribute_name']
            )->join(
                array('eas' => $eavAttributeSetTable),
                'eea.attribute_set_id = eas.attribute_set_id',
                ['attribute_set_name']
            )
            ->where('eas.attribute_set_name LIKE :oro_prefix_name')
            ->where('ea.attribute_code LIKE :attribute_code')
            ->order('eea.attribute_set_id ASC');

            $bind = [
                ':oro_prefix_name' => sprintf('%s_%%', 'Oro'),
                ':attribute_code' => sprintf('%s_%%', 'oro'),
            ];

        $relationsData = $connection->fetchAll($selectAttributeToSet, $bind);

        return $relationsData;
    }

    /**
     * @param $attributesCollection
     */
    protected function prepareCsv($attributesCollection)
    {
        $path = static::EXPORT_FILE_NAME;
        $file = fopen($path, 'w');

        $first = true;
        foreach ($attributesCollection as $line) {
            if ($first) {
                $titles = array();
                foreach ($line as $field => $val) {
                    $titles[] = $field;
                }
                fputcsv($file, $titles);
                $first = false;
            }
            fputcsv($file, $line);
        }
    }

    /**
     * Returns table name
     *
     * @param string|string[] $tableName
     * @return string
     */
    protected function getTable($tableName)
    {
        return $this->resource->getTableName($tableName);
    }

}
