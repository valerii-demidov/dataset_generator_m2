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
class ExportAttributes extends Command
{
    const EXPORT_FILE_NAME = 'export_attributes.csv';
    const CATEGORY_PARENT_ID = 2;

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
        $this->setName('exportattributes:run')
            ->setDescription('Run Export Of Attributes');


        parent::configure();
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $attributesCollection = $this->prepareAttributeCollection();
        $this->prepareCsv($attributesCollection);
    }

    /**
     * @return mixed
     */
    protected function prepareAttributeCollection()
    {
        $connection = $this->resourceConnection->getConnection();

        $eavAttributeTable = $this->getTable('eav_attribute');
        $catalogEavAttributeTable = $this->getTable('catalog_eav_attribute');
        $eavAttributeOptionTable = $this->getTable('eav_attribute_option');
        $eavAttributeOptionValue = $this->getTable('eav_attribute_option_value');

        $selectAttributes = $connection->select()
            ->from(array('ea' => $eavAttributeTable))
            ->join(
                array('c_ea' => $catalogEavAttributeTable),
                'ea.attribute_id = c_ea.attribute_id'
            );

        $selectProdAttributes = $selectAttributes->where('ea.entity_type_id = '.$this->catalogProductEntityTypeId)
            ->order('ea.attribute_id ASC');


        $productAttributes = $connection->fetchAll($selectProdAttributes);
        $selectAttributeOption = $selectProdAttributes
            ->join(
                array('e_ao' => $eavAttributeOptionTable, array('option_id')),
                'c_ea.attribute_id = e_ao.attribute_id'
            )
            ->join(
                array('e_aov' => $eavAttributeOptionValue, array('value')),
                'e_ao.option_id = e_aov.option_id and store_id = 0'
            )
            ->order('e_ao.attribute_id ASC');

        $productAttributeOptions = $connection->fetchAll($selectAttributeOption);
        $attributesCollection = $this->mergeCollections($productAttributes, $productAttributeOptions);

        return $attributesCollection;
    }

    /**
     * @param $productAttributes
     * @param $productAttributeOptions
     * @return mixed
     */
    protected function mergeCollections($productAttributes, $productAttributeOptions)
    {

        foreach ($productAttributes as $key => $_prodAttrib) {
            $values = array();
            $attribId = $_prodAttrib['attribute_id'];
            foreach ($productAttributeOptions as $pao) {
                if ($pao['attribute_id'] == $attribId) {
                    $values[] = $pao['value'];
                }
            }
            if (count($values) > 0) {
                $values = implode(";", $values);
                $productAttributes[$key]['_options'] = $values;
            } else {
                $productAttributes[$key]['_options'] = "";
            }
            /*
                temp
            */
            $productAttributes[$key]['attribute_code'] = $productAttributes[$key]['attribute_code'];
        }

        return $productAttributes;
    }

    /**
     * @param $attributesCollection
     */
    protected function prepareCsv($attributesCollection){

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