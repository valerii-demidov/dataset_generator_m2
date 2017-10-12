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
use Symfony\Component\Config\Definition\Exception\Exception;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;


/**
 * Command for executing dataset generation
 */
class DatasetCommand extends Command
{
    /**
     * Name of input option
     */
    const INPUT_KEY_CONFIG = 'config';

    /**
     * Object manager factory
     *
     * @var ObjectManagerFactory
     */
    private $objectManagerFactory;

    /**
     * DataSet Configuration
     *
     * @var \stdClass
     */
    protected $_config;

    /**
     * @var \Magento\Framework\Filesystem
     */
    protected $_fileSystem;

    /**
     * Output interface instance
     *
     * @var OutputInterface
     */
    protected $_output;

    /**
     * Data Output File Stream Resource
     *
     * @var \Magento\Framework\Filesystem\File\WriteInterface
     */
    protected $_streamOutputData;

    /**
     * Image Output File Stream Resource
     *
     * @var \Magento\Framework\Filesystem\File\WriteInterface
     */
    protected $_streamOutputImage;

    /**
     * Database adapter instance
     *
     * @var \Magento\Framework\DB\Adapter\AdapterInterface
     */
    protected $_adapter;

    /**
     * Core Resource Instance
     *
     * @var \Magento\Framework\App\ResourceConnection
     */
    protected $_resource;

    /**
     * EAV Configuration instance
     *
     * @var \Magento\Eav\Model\Config
     */
    protected $_eavConfig;

    /**
     * Application configuration instance
     *
     * @var \Magento\Framework\App\Config\ScopeConfigInterface
     */
    protected $_appConfig;

    /**
     * Application Store Manager
     *
     * @var \Magento\Store\Model\StoreManagerInterface
     */
    protected $_appStore;

    /**
     * @var int
     */
    protected $_storeId;
    protected $_websiteId;

    /**
     * @var string
     */
    protected $_catalogProductSuffix;
    protected $_catalogCategorySuffix;

    /**
     * @var string
     */
    protected $_prefixCode;
    protected $_prefixName;

    /**
     * @var int
     */
    protected $_catalogProductEntityTypeId;
    protected $_catalogCategoryEntityTypeId;
    protected $_catalogProductGalleryAttributeId;

    /**
     * @var int
     */
    protected $_maxEavAttributeSetId;
    protected $_maxEavAttributeId;
    protected $_maxEavAttributeGroupId;
    protected $_maxEavAttributeOptionId;
    protected $_maxCatalogProductEntityId;
    protected $_maxCatalogCategoryEntityId;
    protected $_maxCatalogProductMediaGallery;
    protected $_maxCatalogProductLinkId;
    protected $_maxCatalogProductSuperAttributeId;
    protected $_maxCustomerGroupGroupId;

    /**
     * @var array
     */
    protected $_customerGroups                          = [];
    protected $_catalogProductAttributeSets             = [];
    protected $_catalogProductAttributeGroups           = [];
    protected $_catalogProductAttributes                = [];
    protected $_catalogProductAttributeMap              = [];
    protected $_catalogProductAttributeSetMap           = [];
    protected $_catalogCategories                       = [];
    protected $_catalogProductDefaultAttributeSetData;
    protected $_catalogProductLinkAttributes;
    protected $_catalogProductSimple                    = [];
    protected $_catalogProductSimpleSuper               = [];
    protected $_urlRewrites                             = [];

    /**
     * @var \Magento\Eav\Model\Attribute[]
     */
    protected $_catalogProductAttributeCache            = [];

    /**
     * @var array
     */
    protected $_bufferCatalogCategoryEntity             = [];
    protected $_bufferCatalogCategoryAttributes         = [];
    protected $_bufferCatalogProductEntity              = [];
    protected $_bufferCatalogProductEntitySequence      = [];
    protected $_bufferCatalogProductWebsite             = [];
    protected $_bufferCatalogProductCategory            = [];
    protected $_bufferCatalogProductPriceGroup          = [];
    protected $_bufferCatalogProductPriceTier           = [];
    protected $_bufferCatalogProductAttributes          = [];
    protected $_bufferCatalogProductMediaGallery        = [];
    protected $_bufferCatalogProductMediaValue          = [];
    protected $_bufferCatalogProductMediaValueToEntity  = [];
    protected $_bufferCatalogInventoryStockItem         = [];
    protected $_bufferCatalogProductLink                = [];
    protected $_bufferCatalogProductSuper               = [];
    protected $_bufferCatalogProductRelation            = [];
    protected $_bufferUrlRewrite                        = [];

    /**
     * Constructor
     *
     * @param ObjectManagerFactory $objectManagerFactory
     */
    public function __construct(ObjectManagerFactory $objectManagerFactory)
    {
        $this->objectManagerFactory = $objectManagerFactory;
        parent::__construct();
    }

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $options = [
            new InputOption(
                self::INPUT_KEY_CONFIG,
                null,
                InputOption::VALUE_REQUIRED,
                'Dataset configuration file'
            ),
        ];
        $this->setName('dataset:run')
            ->setDescription('Runs dataset generation')
            ->setDefinition($options);

        parent::configure();
    }

    /**
     * {@inheritdoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $omParams = $_SERVER;
        $omParams[Store::CUSTOM_ENTRY_POINT_PARAM] = true;
        $objectManager = $this->objectManagerFactory->create($omParams);

        $this->_resource    = $objectManager->get('\Magento\Framework\App\ResourceConnection');
        $this->_adapter     = $this->_resource->getConnection();
        $this->_eavConfig   = $objectManager->get('\Magento\Eav\Model\Config');
        $this->_appConfig   = $objectManager->get('\Magento\Framework\App\Config\ScopeConfigInterface');
        $this->_appStore    = $objectManager->get('\Magento\Store\Model\StoreManagerInterface');
        $this->_fileSystem  = $objectManager->create('Magento\Framework\Filesystem');
        $this->_output      = $output;

        $this->_validateConfig($objectManager, $input);
        $this->_initConfig();
        $this->_initOutput();

        $this->_prepareCustomerGroups();
        $this->_prepareCatalogProductAttributeSets();
        $this->_prepareCatalogProductAttributeGroups();
        $this->_prepareCatalogProductAttributes();
        $this->_prepareCatalogProductAttributeEntity();
        $this->_prepareCatalogCategories(2);
        $this->_mapCatalogCategoryToCatalogProductSet();
        $this->_prepareCatalogProductSimple();
        $this->_prepareCatalogProductGrouped();
        $this->_prepareCatalogProductConfigurable();

        $this->_endOutput();
        $output->writeln('<info>' . '[Finished]' . '</info>');
    }

    /**
     * Validates and Reads Configuration file
     *
     * @param ObjectManagerInterface $objectManager
     * @param InputInterface $input
     * @throws \InvalidArgumentException
     */
    protected function _validateConfig(ObjectManagerInterface $objectManager, InputInterface $input)
    {
        $configFile = $input->getOption(self::INPUT_KEY_CONFIG);
        if (empty($configFile)) {
            throw new \InvalidArgumentException('Missing Configuration File');
        }

        $dir = $this->_fileSystem->getDirectoryRead(DirectoryList::ROOT);
        if (!$dir->isFile($configFile)) {
            throw new \InvalidArgumentException('Invalid Path to Configuration File');
        }

        $content = $dir->readFile($configFile);
        try {
            $this->_config = json_decode($content);
        } catch (Exception $e) {
            throw new \InvalidArgumentException(sprintf('Invalid Configuration File %s', $e->getMessage()));
        }
    }

    /**
     * Returns table name
     *
     * @param string|string[] $tableName
     * @return string
     */
    protected function _getTable($tableName)
    {
        return $this->_resource->getTableName($tableName);
    }

    /**
     * Initializes application configuration
     */
    protected function _initConfig()
    {
        $this->_prefixCode = !empty($this->_config->prefix->code) ? $this->_config->prefix->code : 'oro';
        $this->_prefixName = !empty($this->_config->prefix->name) ? $this->_config->prefix->name : 'Oro';

        // MAX ID
        $this->_maxEavAttributeSetId                = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('eav_attribute_set'), 'MAX(attribute_set_id)'));
        $this->_maxEavAttributeGroupId              = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('eav_attribute_group'), 'MAX(attribute_group_id)'));
        $this->_maxEavAttributeId                   = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('eav_attribute'), 'MAX(attribute_id)'));
        $this->_maxEavAttributeOptionId             = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('eav_attribute_option'), 'MAX(option_id)'));
        $this->_maxCatalogCategoryEntityId          = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('catalog_category_entity'), 'MAX(entity_id)'));
        $this->_maxCatalogProductEntityId           = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('catalog_product_entity'), 'MAX(entity_id)'));
        $this->_maxCatalogProductMediaGallery       = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('catalog_product_entity_media_gallery'), 'MAX(value_id)'));
        $this->_maxCatalogProductLinkId             = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('catalog_product_link'), 'MAX(link_id)'));
        $this->_maxCatalogProductSuperAttributeId   = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('catalog_product_super_attribute'), 'MAX(product_super_attribute_id)'));
        $this->_maxCustomerGroupGroupId             = (int)$this->_adapter->fetchOne($this->_adapter->select()
            ->from($this->_getTable('customer_group'), 'MAX(customer_group_id)'));

        $this->_catalogCategoryEntityTypeId     = (int) $this->_eavConfig->getEntityType('catalog_category')->getId();
        $this->_catalogProductEntityTypeId      = (int) $this->_eavConfig->getEntityType('catalog_product')->getId();

        $this->_catalogProductSuffix  = $this->_appConfig
            ->getValue(ProductUrlPathGenerator::XML_PATH_PRODUCT_URL_SUFFIX);
        $this->_catalogCategorySuffix = $this->_appConfig
            ->getValue(CategoryUrlPathGenerator::XML_PATH_CATEGORY_URL_SUFFIX);

        $store = $this->_appStore->getDefaultStoreView();
        $this->_storeId   = (int) $store->getId();
        $this->_websiteId = (int) $store->getWebsiteId();
    }

    /**
     * Initializes output stream handles
     */
    protected function _initOutput()
    {
        $dir = $this->_fileSystem->getDirectoryWrite(DirectoryList::ROOT);

        $this->_streamOutputData  = $dir->openFile($this->_config->output, 'w+');
        $instructions = [
            '/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;',
            '/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;',
            '/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;',
            '/*!40101 SET NAMES utf8 */;',
            '/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;',
            '/*!40103 SET TIME_ZONE=\'+00:00\' */;',
            '/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;',
            '/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;',
            '/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE=\'NO_AUTO_VALUE_ON_ZERO\' */;',
            '/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;',
        ];
        foreach ($instructions as $instruction) {
            $this->_saveData($instruction . PHP_EOL);
        }
        $this->_saveData(PHP_EOL);

        $imageConfig = $this->_config->catalog->product->image;
        if ($imageConfig) {
            $this->_streamOutputImage = $dir->openFile($imageConfig->output, 'w+');
        }
    }

    /**
     * Closes output stream handles
     */
    protected function _endOutput()
    {
        if ($this->_streamOutputData) {
            $instructions = [
                '/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;',
                '/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;',
                '/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;',
                '/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;',
                '/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;',
                '/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;',
                '/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;',
                '/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;'
            ];

            $this->_saveData(PHP_EOL);
            foreach ($instructions as $instruction) {
                $this->_saveData($instruction . PHP_EOL);
            }
            $this->_streamOutputData->close();

            $this->_output->write("Database instruction file: " . $this->_config->output);
        }

        if ($this->_streamOutputImage) {
            $this->_streamOutputImage->close();
            $this->_output->write("Image symlinks file: " . $this->_config->catalog->product->image->output);
        }
    }

    /**
     * Writes result to output file
     *
     * @param $string
     */
    protected function _saveData($string)
    {
        $this->_streamOutputData->write($string);
    }

    /**
     * Writes result to output file
     *
     * @param $string
     */
    protected function _saveImage($string)
    {
        $this->_streamOutputImage->write($string);
    }

    /**
     * Generates INSERT SQL query
     *
     * @param string $table
     * @param array $data
     * @param bool $onlyValues
     * @return string
     */
    protected function _getInsertSql($table, $data, $onlyValues = true)
    {
        $part = sprintf('INSERT INTO %s ', $this->_adapter->quoteIdentifier($table));
        if (!$onlyValues) {
            $columns    = [];
            foreach (array_keys($data) as $column) {
                $columns[] = $this->_adapter->quoteIdentifier($column);
            }
            $part .= sprintf('(%s) ', implode(', ', $columns));
        }
        $values = [];
        foreach ($data as $value) {
            if ($value === null) {
                $value = 'NULL';
            } else {
                $value = $this->_adapter->quote($value);
            }
            $values[] = $value;
        }
        $part .= sprintf('VALUES(%s);', implode(', ', $values));

        return $part . PHP_EOL;
    }

    /**
     * Generates insert multiply rows with specified data into table
     *
     * @param string $table
     * @param array $data
     * @return string
     */
    protected function _getInsertMultiplySql($table, array $data)
    {
        $row = reset($data);
        // support insert syntaxes
        if (!is_array($row)) {
            return $this->_getInsertSql($table, $data);
        }

        // validate data array
        $cols = array_keys($row);
        $insertArray = [];
        foreach ($data as $row) {
            $line = [];
            if (array_diff($cols, array_keys($row))) {
                continue;
            }
            foreach ($cols as $field) {
                if ($row[$field] === null) {
                    $value = 'NULL';
                } else {
                    $value = $this->_adapter->quote($row[$field]);
                }
                $line[] = $value;
            }
            $insertArray[] = sprintf('(%s)', implode(', ', $line));
        }
        unset($row);

        $columns    = [];
        foreach ($cols as $column) {
            $columns[] = $this->_adapter->quoteIdentifier($column);
        }
        return sprintf('INSERT INTO %s (%s) VALUES %s;',
            $this->_adapter->quoteIdentifier($table),
            implode(', ', $columns),
            implode(',', $insertArray)) . PHP_EOL;
    }


    /**
     * Formats execution time to human readable format
     *
     * @param float $value
     * @return string
     */
    protected function _formatTime($value)
    {
        if ($value < 60) {
            $hh = '0';
            $mm = '0';
            $ss = ceil($value);
        } else if ($value < 3600) {
            $ss = $value % 60;
            $mm = ($value - $ss) / 60;
            $hh = '0';
        } else {
            $ss = $value % 60;
            $mm = (($value - $ss) % 3600) / 60;
            $hh = ($value - $mm * 60 - $ss) / 3600;
        }

        return sprintf('%02d:%02d:%02d', $hh, $mm, $ss);
    }

    /**
     * Returns Random string
     *
     * @param int $min
     * @param int $max
     * @param int $maxLength
     * @return string
     */
    protected function _getRandomWords($min, $max, $maxLength)
    {
        $words   = [];
        $count   = mt_rand($min, $max);
        $pattern = 'aaabcdeeefghiiijklmnooopqrstuuuvwxyzaaabcdeeefghiiijklmnooopqrstuuuvwxyz';
        for ($i = 0; $i < $count; $i ++) {
            $length  = mt_rand(2, $maxLength);
            $word    = substr(str_shuffle(str_repeat($pattern, mt_rand(1, 10))), 0, $length);
            if ($i === 0 || mt_rand(1, 4) === 2) {
                $word = ucfirst($word);
            }
            $words[] = $word;
        }

        return implode(' ', $words);
    }

    /**
     * Returns random text attribute value
     *
     * @return string
     */
    protected function _renderTextAttributeValue()
    {
        return $this->_getRandomWords(2, 10, 12);
    }

    /**
     * Returns random textarea attribute value
     *
     * @return string
     */
    protected function _renderTextAreaAttributeValue()
    {
        return $this->_getRandomWords(10, 30, 15);
    }

    /**
     * Returns random product SKU based on prefix and index
     *
     * @param int $index
     * @return string
     */
    protected function _renderCatalogProductSku($index)
    {
        return strtoupper(sprintf('%s-%08d-%s', $this->_prefixCode, $index,
            substr(strtr(base64_encode(mcrypt_create_iv(8)), ['+' => '', '/' => '']), 0, 8)));
    }

    /**
     * Prepares Customer Groups
     */
    protected function _prepareCustomerGroups()
    {
        $timer  = microtime(true);
        $this->_output->write('Preparing customer groups' . PHP_EOL);

        $table  = $this->_getTable('customer_group');
        $select = $this->_adapter->select()
            ->from($table, ['customer_group_id'])
            ->where('customer_group_id != 0');

        $this->_customerGroups = $this->_adapter->fetchCol($select);

        $count = $this->_config->customer->group->count;
        if (!$count || count($this->_customerGroups) >= $count) {
            $this->_output->write('Customer groups already exists' . PHP_EOL);
            return;
        }

        for ($i = count($this->_customerGroups); $i <= $count; $i ++) {
            $groupId   = ++ $this->_maxCustomerGroupGroupId;
            $groupName = sprintf('%s Group %02d', $this->_prefixName, $i);
            $groupData = [
                'customer_group_id'   => $groupId,
                'customer_group_code' => $groupName,
                'tax_class_id'        => 0,
            ];

            $this->_saveData($this->_getInsertSql($table, $groupData));

            $this->_customerGroups[] = $groupId;
        }

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Customer groups have been generated in %s', $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Prepares Attribute Sets
     */
    protected function _prepareCatalogProductAttributeSets()
    {
        $timer  = microtime(true);
        $this->_output->write('Preparing catalog product attribute sets' . PHP_EOL);

        $select = $this->_adapter->select()
            ->from($this->_getTable('eav_attribute_set'), ['attribute_set_name', 'attribute_set_id'])
            ->where('entity_type_id = :entity_type_id')
            ->where('attribute_set_name LIKE :name_mask');
        $bind   = [
            ':entity_type_id' => $this->_catalogProductEntityTypeId,
            ':name_mask'      => $this->_prefixName . ' Set%',
        ];

        $this->_catalogProductAttributeSets = $this->_adapter->fetchPairs($select, $bind);

        $nameTpl    = sprintf('%s Set #%%02d', $this->_prefixName);
        $count      = $this->_config->catalog->attribute->set->count;
        $tableSet   = $this->_getTable('eav_attribute_set');
        $tableGroup = $this->_getTable('eav_attribute_group');
        $tableRel   = $this->_getTable('eav_entity_attribute');
        for ($i = 1; $i <= $count; $i ++) {
            $setName = sprintf($nameTpl, $i);
            if (!isset($this->_catalogProductAttributeSets[$setName])) {
                $setId  = ++ $this->_maxEavAttributeSetId;
                $data   = [
                    'attribute_set_id'      => $setId,
                    'entity_type_id'        => (int)$this->_catalogProductEntityTypeId,
                    'attribute_set_name'    => $setName,
                    'sort_order'            => $i + 10,
                ];
                $sql    = $this->_getInsertSql($tableSet, $data);
                $this->_saveData($sql);

                $this->_catalogProductAttributeSets[$setName] = $setId;

                $setData = $this->_getDefaultCatalogProductAttributeSetData();
                foreach ($setData as $groupData) {
                    $groupId    = ++ $this->_maxEavAttributeGroupId;
                    $attributes = $groupData['attributes'];
                    unset($groupData['attributes']);
                    $groupData['attribute_group_id'] = $groupId;
                    $groupData['attribute_set_id']   = $setId;

                    $sql        = $this->_getInsertSql($tableGroup, $groupData, false);
                    $this->_saveData($sql);

                    foreach ($attributes as $attributeId => $sortOrder) {
                        $entityData = [
                            'entity_attribute_id'   => null,
                            'entity_type_id'        => $this->_catalogProductEntityTypeId,
                            'attribute_set_id'      => $setId,
                            'attribute_group_id'    => $groupId,
                            'attribute_id'          => (int)$attributeId,
                            'sort_order'            => (int)$sortOrder,
                        ];

                        $sql        = $this->_getInsertSql($tableRel, $entityData);
                        $this->_saveData($sql);
                    }
                }
            }
        }

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Catalog product attribute sets have been generated in %s', $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Returns Catalog Product Default Attribute Set Data
     *
     * @return array
     */
    protected function _getDefaultCatalogProductAttributeSetData()
    {
        if ($this->_catalogProductDefaultAttributeSetData === null) {
            $result = [];
            $select = $this->_adapter->select()
                ->from($this->_getTable('eav_attribute_set'), 'attribute_set_id')
                ->where('entity_type_id = :entity_type_id')
                ->where('attribute_set_name = :set_name');
            $bind   = [
                ':entity_type_id' => $this->_catalogProductEntityTypeId,
                ':set_name'       => 'Default',
            ];

            $setId = (int) $this->_adapter->fetchOne($select, $bind);

            $select = $this->_adapter->select()
                ->from($this->_getTable('eav_attribute_group'))
                ->where('attribute_set_id = :attribute_set_id')
                ->order('sort_order');
            $bind   = [
                ':attribute_set_id' => $setId,
            ];

            foreach ($this->_adapter->fetchAll($select, $bind) as $row) {
                $result[$row['attribute_group_id']] = [
                    'attribute_group_id'   => null,
                    'attribute_set_id'     => null,
                    'tab_group_code'       => $row['tab_group_code'],
                    'sort_order'           => (int) $row['sort_order'],
                    'default_id'           => (int) $row['default_id'],
                    'attribute_group_code' => $row['attribute_group_code'],
                    'attribute_group_name' => $row['attribute_group_name'],
                    'attributes'           => [],
                ];
            }

            $select = $this->_adapter->select()
                ->from($this->_getTable('eav_entity_attribute'))
                ->where('attribute_set_id = :attribute_set_id')
                ->where('entity_type_id = :entity_type_id')
                ->order('sort_order');
            $bind   = [
                ':entity_type_id'   => $this->_catalogProductEntityTypeId,
                ':attribute_set_id' => $setId,
            ];

            foreach ($this->_adapter->fetchAll($select, $bind) as $row) {
                if (!isset($result[$row['attribute_group_id']])) {
                    continue;
                }
                $result[$row['attribute_group_id']]['attributes'][$row['attribute_id']] = $row['sort_order'];
            }

            $this->_catalogProductDefaultAttributeSetData = $result;
        }

        return $this->_catalogProductDefaultAttributeSetData;
    }

    /**
     * Prepares Catalog Product Custom Attribute Groups
     */
    protected function _prepareCatalogProductAttributeGroups()
    {
        $timer  = microtime(true);
        $this->_output->write('Preparing catalog product attribute set groups' . PHP_EOL);

        $select = $this->_adapter->select()
            ->from(['eag' => $this->_getTable('eav_attribute_group')], ['attribute_set_id', 'attribute_group_id'])
            ->joinInner(
                ['eas' => $this->_getTable('eav_attribute_set')],
                'eag.attribute_set_id = eas.attribute_set_id AND eas.entity_type_id = :entity_type_id',
                null)
            ->where('entity_type_id = :entity_type_id')
            ->where('eag.attribute_group_name = :group_name');

        $bind   = [
            ':entity_type_id' => $this->_catalogProductEntityTypeId,
            ':group_name'     => $this->_prefixName,
        ];

        $this->_catalogProductAttributeGroups = $this->_adapter->fetchPairs($select, $bind);

        foreach ($this->_catalogProductAttributeSets as $setId) {
            if (!isset($this->_catalogProductAttributeGroups[$setId])) {
                $groupId = ++ $this->_maxEavAttributeGroupId;
                $data    = [
                    'attribute_group_id'   => $groupId,
                    'attribute_set_id'     => (int)$setId,
                    'attribute_group_name' => $this->_prefixName,
                    'sort_order'           => 10,
                    'default_id'           => 0,
                    'attribute_group_code' => $this->_prefixCode,
                    'tab_group_code'       => 'basic',
                ];
                $sql    = $this->_getInsertSql($this->_getTable('eav_attribute_group'), $data);
                $this->_saveData($sql);

                $this->_catalogProductAttributeGroups[$setId] = $groupId;
            }
        }

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Catalog product attribute set groups have been generated in %s',
                $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Prepares Catalog Product Attributes
     */
    protected function _prepareCatalogProductAttributes()
    {
        $timer  = microtime(true);
        $this->_output->write('Preparing catalog product attributes' . PHP_EOL);

        $select = $this->_adapter->select()
            ->from(['ea' => $this->_getTable('eav_attribute')],
                ['attribute_id', 'attribute_code', 'frontend_input', 'backend_type', 'frontend_label'])
            ->joinInner(
                ['cea' => $this->_getTable('catalog_eav_attribute')],
                'ea.attribute_id = cea.attribute_id',
                ['is_filterable'])
            ->where('ea.entity_type_id = :entity_type_id')
            ->where('ea.attribute_code LIKE :code_mask');
        $bind   = [
            ':entity_type_id' => $this->_catalogProductEntityTypeId,
            ':code_mask'      => sprintf('%s_%%', $this->_prefixCode),
        ];

        $loadOptions    = [];
        foreach ($this->_adapter->fetchAll($select, $bind) as $row) {
            $this->_catalogProductAttributes[$row['attribute_code']] = [
                'attribute_id'      => (int)$row['attribute_id'],
                'frontend_input'    => $row['frontend_input'],
                'backend_type'      => $row['backend_type'],
                'is_filterable'     => (int)$row['is_filterable'],
                'frontend_label'    => $row['frontend_label'],
                'options'           => [],
            ];
            $this->_catalogProductAttributeMap[$row['attribute_id']] = $row['attribute_code'];
            if ($row['frontend_input'] === 'select') {
                $loadOptions[$row['attribute_id']] = $row['attribute_code'];
            }
        }

        if ($loadOptions) {
            $select = $this->_adapter->select()
                ->from($this->_getTable('eav_attribute_option'), ['attribute_id', 'option_id'])
                ->where('attribute_id IN(?)', array_keys($loadOptions));
            foreach ($this->_adapter->fetchAll($select) as $row) {
                $attributeCode = $loadOptions[$row['attribute_id']];
                $this->_catalogProductAttributes[$attributeCode]['options'][] = $row['option_id'];
            }
        }

        foreach (['regular', 'filterable'] as $attributeType) {
            $count   = $this->_config->catalog->attribute->$attributeType;
            $codeTpl = sprintf('%s_%s_%%02d', $this->_prefixCode, $attributeType);
            for ($i = 1; $i <= $count; $i ++) {
                $attributeCode = sprintf($codeTpl, $i);
                if (!isset($this->_catalogProductAttributes[$attributeCode])) {
                    // create attribute
                    $isFilterable   = 0;
                    $options        = [];
                    $sourceModel    = null;
                    $backendModel   = null;
                    if ($attributeType === 'filterable') {
                        $frontendType   = 'select';
                        $backendType    = 'int';
                        $sourceModel    = 'Magento\Eav\Model\Entity\Attribute\Source\Table';
                        $isFilterable   = 1;
                    } else {
                        switch(mt_rand(1, 8)) {
                            case 1:
                            case 5:
                                $frontendType   = 'select';
                                $backendType    = 'int';
                                $sourceModel    = 'Magento\Eav\Model\Entity\Attribute\Source\Table';
                                break;
                            case 7:
                                $frontendType   = 'boolean';
                                $backendType    = 'int';
                                $sourceModel    = 'Magento\Eav\Model\Entity\Attribute\Source\Boolean';
                                break;
                            case 4:
                                $frontendType   = 'textarea';
                                $backendType    = 'text';
                                break;
                            default:
                                $frontendType   = 'text';
                                $backendType    = 'varchar';
                                break;
                        }
                    }

                    $attributeId    = ++ $this->_maxEavAttributeId;
                    $attributeData  = [
                        'attribute_id'    => $attributeId,
                        'entity_type_id'  => $this->_catalogProductEntityTypeId,
                        'attribute_code'  => $attributeCode,
                        'attribute_model' => null,
                        'backend_model'   => $backendModel,
                        'backend_type'    => $backendType,
                        'backend_table'   => null,
                        'frontend_model'  => null,
                        'frontend_input'  => $frontendType,
                        'frontend_label'  => sprintf('%s %02d', ucfirst($attributeType), $i),
                        'frontend_class'  => null,
                        'source_model'    => $sourceModel,
                        'is_required'     => 0,
                        'is_user_defined' => 1,
                        'default_value'   => null,
                        'is_unique'       => 0,
                        'note'            => null,
                    ];

                    $this->_saveData($this->_getInsertSql($this->_getTable('eav_attribute'), $attributeData));

                    $catalogData    = [
                        'attribute_id'                  => $attributeId,
                        'frontend_input_renderer'       => null,
                        'is_global'                     => 1,
                        'is_visible'                    => 1,
                        'is_searchable'                 => 0,
                        'is_filterable'                 => $isFilterable,
                        'is_comparable'                 => 0,
                        'is_visible_on_front'           => 1,
                        'is_html_allowed_on_front'      => 0,
                        'is_used_for_price_rules'       => 0,
                        'is_filterable_in_search'       => $isFilterable,
                        'used_in_product_listing'       => 0,
                        'used_for_sort_by'              => 0,
                        'apply_to'                      => null,
                        'is_visible_in_advanced_search' => 0,
                        'position'                      => 0,
                        'is_wysiwyg_enabled'            => 0,
                        'is_used_for_promo_rules'       => 0,
                        'is_required_in_admin_store'    => 0,
                        'is_used_in_grid'               => $isFilterable,
                        'is_visible_in_grid'            => 0,
                        'is_filterable_in_grid'         => $isFilterable,
                        'search_weight'                 => 2,
                        'additional_data'               => null,
                    ];
                    $this->_saveData($this->_getInsertSql($this->_getTable('catalog_eav_attribute'), $catalogData, false));

                    if ($frontendType === 'select') {
                        $countOptions   = mt_rand($this->_config->catalog->attribute->options->min,
                            $this->_config->catalog->attribute->options->max);
                        $tableOption    = $this->_getTable('eav_attribute_option');
                        $tableOptionVal = $this->_getTable('eav_attribute_option_value');
                        for ($j = 1; $j <= $countOptions; $j ++) {
                            $optionId    = ++ $this->_maxEavAttributeOptionId;
                            $optionValue = sprintf('Option %02d-%03d', $i, $j);
                            $optionData = [
                                'option_id'    => $optionId,
                                'attribute_id' => $attributeId,
                                'sort_order'   => $j,
                            ];
                            $this->_saveData($this->_getInsertSql($tableOption, $optionData));

                            $optionValueData = [
                                'value_id'  => null,
                                'option_id' => $optionId,
                                'store_id'  => 0,
                                'value'     => $optionValue,
                            ];
                            $this->_saveData($this->_getInsertSql($tableOptionVal, $optionValueData));
                            $options[] = $optionId;
                        }
                    }

                    $this->_catalogProductAttributes[$attributeCode] = [
                        'attribute_id'      => $attributeId,
                        'frontend_input'    => $frontendType,
                        'backend_type'      => $backendType,
                        'is_filterable'     => $isFilterable,
                        'frontend_label'    => $attributeData['frontend_label'],
                        'options'           => $options,
                    ];
                    $this->_catalogProductAttributeMap[$attributeId] = $attributeCode;
                }
            }
        }

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Catalog product attributes have been generated in %s', $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Prepares relation between Catalog Product Attribute and their Sets
     */
    protected function _prepareCatalogProductAttributeEntity()
    {
        $tableRel = $this->_getTable('eav_entity_attribute');
        $select = $this->_adapter->select()
            ->from($tableRel, ['attribute_id', 'attribute_set_id'])
            ->where('attribute_group_id IN(?)', $this->_catalogProductAttributeGroups);

        foreach ($this->_adapter->fetchAll($select) as $row) {
            $this->_catalogProductAttributeSetMap[$row['attribute_set_id']][] = $row['attribute_id'];
        }

        $regular    = [];
        $filterable = [];
        foreach ($this->_catalogProductAttributes as $attributeData) {
            if ($attributeData['is_filterable']) {
                $filterable[] = $attributeData['attribute_id'];
            } else {
                $regular[] = $attributeData['attribute_id'];
            }
        }

        foreach ($this->_catalogProductAttributeGroups as $setId => $groupId) {
            if (empty($this->_catalogProductAttributeSetMap[$setId])) {
                $regularCount         = mt_rand($this->_config->catalog->attribute->set->regular->min,
                    $this->_config->catalog->attribute->set->regular->max);
                $filterableCount      = mt_rand($this->_config->catalog->attribute->set->regular->min,
                    $this->_config->catalog->attribute->set->regular->max);
                shuffle($regular);
                shuffle($filterable);
                $regularAttributes    = array_slice($regular, 0, $regularCount);
                $filterableAttributes = array_slice($filterable, 0  , $filterableCount);
                $sortOrder            = 0;
                $attributes           = array_merge($regularAttributes, $filterableAttributes);

                foreach ($attributes as $attributeId) {
                    $sortOrder ++;
                    $data = [
                        'entity_attribute_id' => null,
                        'entity_type_id'      => $this->_catalogProductEntityTypeId,
                        'attribute_set_id'    => (int) $setId,
                        'attribute_group_id'  => (int) $groupId,
                        'attribute_id'        => (int) $attributeId,
                        'sort_order'          => $sortOrder,
                    ];
                    $this->_saveData($this->_getInsertSql($tableRel, $data));
                }

                $this->_catalogProductAttributeSetMap[$setId] = $attributes;
            }
        }
    }

    /**
     * Prepares Catalog Category structure
     *
     * @param int $rootCategoryId
     */
    protected function _prepareCatalogCategories($rootCategoryId)
    {
        $timer  = microtime(true);
        $this->_output->write('Preparing catalog categories' . PHP_EOL);

        $select = $this->_adapter->select()
            ->from($this->_getTable('catalog_category_entity'))
            ->where('entity_id = :category_id');
        $bind   = [
            ':category_id' => $rootCategoryId,
        ];

        $rootCategory = $this->_adapter->fetchRow($select, $bind);

        $this->_catalogCategories[$rootCategoryId] = [];

        $select = $this->_adapter->select()
            ->from($this->_getTable('catalog_category_entity'), ['entity_id', 'parent_id', 'path', 'level'])
            ->where('path LIKE :path')
            ->order('level')
            ->order('position');
        $bind   = [
            ':path' => sprintf('%s/%%', $rootCategory['path'])
        ];

        $deep   = $this->_config->catalog->category->deep;
        foreach ($this->_adapter->fetchAll($select, $bind) as $row) {
            $level = $row['level'] - 1;
            if ($deep < $level || $level === 1) {
                $this->_catalogCategories[$rootCategoryId][$row['entity_id']] = [];
                continue;
            }
            $paths = explode('/', $row['path']);
            $root  = $paths[2];
            $this->_catalogCategories[$rootCategoryId][$root][$level][] = $row['entity_id'];
        }

        $count  = count($this->_catalogCategories[$rootCategoryId]);
        $top    = $this->_config->catalog->category->top;

        if ($top <= $count) {
            $finish = microtime(true);
            $this->_output->write([
                sprintf('Catalog categories loaded in %s', $this->_formatTime($finish - $timer)),
                PHP_EOL,
            ]);
            return;
        }

        $this->_output->write('Starting to generate catalog categories' . PHP_EOL);

        $this->_generateCatalogCategories($rootCategory, 1, 0);

        $this->_flushCatalogCategory();

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Catalog categories have been generated in %s', $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Generates Catalog Categories (recursive)
     *
     * @param array $parent
     * @param int $level
     * @param int $index
     * @param string $urlPrefix
     */
    protected function _generateCatalogCategories($parent, $level, $index, $urlPrefix = null)
    {
        $paths      = explode('/', $parent['path']);
        $rootId     = (int) $paths[1];
        $deep       = $this->_config->catalog->category->deep;
        $parentId   = $parent['entity_id'];

        if ($level === 1) {
            $count = $this->_config->catalog->category->top;
        } else {
            $count = mt_rand($this->_config->catalog->category->min, $this->_config->catalog->category->max);
        }

        for ($i = 1; $i <= $count; $i ++) {
            $entityId   = ++ $this->_maxCatalogCategoryEntityId;
            $entityData = [
                'entity_id'        => $entityId,
                'created_in'       => 1,
                'updated_in'       => time(),
                'attribute_set_id' => $parent['attribute_set_id'],
                'parent_id'        => $parentId,
                'created_at'       => '2015-01-01 00:00:00',
                'updated_at'       => '2015-01-01 00:00:00',
                'path'             => sprintf('%s/%d', $parent['path'], $entityId),
                'position'         => $i,
                'level'            => $level + 1,
                'children_count'   => $deep > $level ? 1 : 0,
            ];

            if ($level === 1) {
                $name   = sprintf('Top Category %02d', $i);
                $urlKey = sprintf('top-cat-%02d', $i);
            } else {
                $name   = sprintf('Category Lev %02d %02d/%02d', $level, $index, $i);
                $urlKey = sprintf('cat-%02d-%02d-%02d', $level, $index, $i);
            }
            $urlPath = $urlKey;
            if ($urlPrefix) {
                $urlPath = sprintf('%s/%s', $urlPrefix, $urlKey);
            }

            $attributes = [
                'name'                       => $name,
                'custom_apply_to_products'   => 0,
                'custom_use_parent_settings' => 0,
                'display_mode'               => 'PRODUCTS',
                'include_in_menu'            => 1,
                'is_active'                  => 1,
                'is_anchor'                  => 1,
                'url_key'                    => $urlKey,
                'url_path'                   => $urlPath,
            ];

            $this->_storeCatalogCategory($entityData, $attributes);

            if ($level > 1) {
                $paths = explode('/', $parent['path']);
                $this->_catalogCategories[$rootId][$paths[2]][$level][] = $entityId;
            }

            if ($deep > $level) {
                $this->_generateCatalogCategories($entityData, $level + 1, $i, $urlPath);
            }
            if ($deep === $level) {
                $this->_urlRewrites[$entityData['entity_id']] = $urlPath;
            }
            if ($level === 1) {
                $this->_flushCatalogCategory();
            }
        }
    }

    /**
     * Stores Catalog Category entity cache
     *
     * @param array $entity
     * @param array $attributes
     */
    protected function _storeCatalogCategory($entity, $attributes)
    {
        $this->_bufferCatalogCategoryEntity[] = $entity;
        foreach ($attributes as $attributeCode => $value) {
            $attribute  = $this->_eavConfig->getAttribute($this->_catalogCategoryEntityTypeId, $attributeCode);
            $table      = $attribute->getBackendTable();
            $data       = [
                'value_id'       => null,
                'attribute_id'   => (int) $attribute->getId(),
                'store_id'       => 0,
                'row_id'      => $entity['entity_id'],
                'value'          => $value,
            ];

            $this->_bufferCatalogCategoryAttributes[$table][] = $data;
        }

        if (array_key_exists('url_path', $attributes)) {
            $urlRequest = sprintf('%s%s', $attributes['url_path'], $this->_catalogCategorySuffix);
            $urlTarget  = sprintf('catalog/category/view/id/%d', $entity['entity_id']);
            $data    = [
                'redirect_type'    => 0,
                'is_autogenerated' => 1,
                'metadata'         => null,
                'description'      => null,
                'store_id'         => $this->_storeId,
                'entity_type'      => 'category',
                'entity_id'        => $entity['entity_id'],
                'request_path'     => $urlRequest,
                'target_path'      => $urlTarget,
            ];

            $this->_bufferUrlRewrite[] = $data;
        }
    }

    /**
     * Writes stored Catalog Category data to output
     */
    protected function _flushCatalogCategory()
    {
        if ($this->_bufferCatalogCategoryEntity) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_category_entity'),
                $this->_bufferCatalogCategoryEntity));
            $this->_bufferCatalogCategoryEntity = [];
        }
        if ($this->_bufferCatalogCategoryAttributes) {
            foreach ($this->_bufferCatalogCategoryAttributes as $table => $data) {
                $this->_saveData($this->_getInsertMultiplySql($table, $data));
                unset($this->_bufferCatalogCategoryAttributes[$table]);
            }
        }

        $this->_flushUrlRewrite();
    }

    /**
     * Writes URL data to output
     */
    protected function _flushUrlRewrite()
    {
        if ($this->_bufferUrlRewrite) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('url_rewrite'),
                $this->_bufferUrlRewrite));
            $this->_bufferUrlRewrite = [];
        }
    }

    /**
     * Assigns product set to top category
     */
    protected function _mapCatalogCategoryToCatalogProductSet()
    {
        $count = count($this->_catalogProductAttributeSets);
        $sets  = array_values($this->_catalogProductAttributeSets);
        foreach ($this->_catalogCategories as $rootId => $rootData) {
            $i = 0;
            foreach ($rootData as $topId => $topData) {
                $this->_catalogCategories[$rootId][$topId]['set_id'] = $sets[$i];
                $i ++;
                if ($i >= $count) {
                    $i = 0;
                }
            }
        }
    }

    /**
     * Generates simple catalog products
     */
    protected function _prepareCatalogProductSimple()
    {
        if (!$this->_config->catalog->product->simple) {
            return;
        }

        $timer  = microtime(true);
        $start  = $timer;
        $this->_output->write('Starting to generate catalog simple products' . PHP_EOL);

        $count = $this->_config->catalog->product->simple->count;
        for ($i = 1; $i <= $count; $i ++) {
            $this->_generateCatalogProductSimple(null, $i);

            if ($i % 500 === 0) {
                $this->_flushCatalogProduct();
            }
            if ($i % 10000 === 0) {
                $finish = microtime(true);
                $this->_output->write([
                    sprintf("\t" . 'Generated %08d simple products in %s', $i, $this->_formatTime($finish - $start)),
                    PHP_EOL,
                ]);
                $start  = $finish;
            }
        }

        $this->_flushCatalogProduct();

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Catalog simple products have been generated in %s', $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Generates Simple Catalog Product
     *
     * @param array|null $topCategory
     * @param int $index
     */
    protected function _generateCatalogProductSimple(array $topCategory = null, $index)
    {
        if ($topCategory === null) {
            $rootId      = array_rand($this->_catalogCategories, 1);
            $topId       = array_rand($this->_catalogCategories[$rootId], 1);
            $topCategory = $this->_catalogCategories[$rootId][$topId];
        }

        $deep           = $this->_config->catalog->category->deep;
        if(!isset($topCategory[$deep])){
            return;
        }
        $categories     = $topCategory[$deep];
        $setId          = $topCategory['set_id'];
        $productConfig  = $this->_config->catalog->product->simple;
        if ($productConfig->start) {
            $index = $productConfig->start + $index;
        }

        $entityId       = ++ $this->_maxCatalogProductEntityId;
        $entity         = [
            'entity_id'        => $entityId,
            'created_in'       => 1,
            'updated_in'       => time(),
//            'entity_type_id'   => $this->_catalogProductEntityTypeId,
            'attribute_set_id' => $setId,
            'type_id'          => 'simple',
            'sku'              => $this->_renderCatalogProductSku($index),
            'has_options'      => 0,
            'required_options' => 0,
            'created_at'       => '2015-01-01 00:00:00',
            'updated_at'       => '2015-01-01 00:00:00',
        ];

        $this->_bufferCatalogProductEntity[] = $entity;
        $this->_bufferCatalogProductEntitySequence[] = ['sequence_value' => $entityId];

        $priceConfig    = $productConfig->price;
        $price          = sprintf('%.4F', mt_rand($priceConfig->min * 100, $priceConfig->max * 100) / 100);
        $status         = mt_rand(1, 100) > $productConfig->disabled ? 1 : 0;
        $attributes = [
            'name'              => $this->_getRandomWords(1, 3, 10),
            'url_key'           => strtolower($entity['sku']),
            'page_layout'       => null,
            'options_container' => 'container1',
            'price'             => $price,
            'weight'            => sprintf('%.4f', mt_rand(10, 200) / 100),
            'status'            => $status,
            'visibility'        => 4,
            'tax_class_id'      => 0,
            'description'       => $this->_getRandomWords(10, 30, 15),
            'short_description' => $this->_getRandomWords(2, 10, 10),
            'quantity_and_stock_status' => 1,
        ];

        $this->_generateCatalogProductImages($entity, $attributes);
        $this->_generateCatalogProductPrices($entity, $attributes, $priceConfig);

        $custom = $this->_getCatalogProductCustomAttributes($setId);
        $this->_storeCatalogProductAttributes($entityId, $attributes, $custom);

        $this->_generateCatalogInventoryStockItem($entityId, $productConfig);
        $this->_generateCatalogProductCategories($entityId, $categories, strtolower($entity['sku']));
        $website = [
            'product_id' => $entityId,
            'website_id' => $this->_websiteId,
        ];

        $this->_bufferCatalogProductWebsite[] = $website;

        if (property_exists($this->_config->catalog->product, 'grouped')) {
            $this->_catalogProductSimple[] = $entityId;
        }

        if ($this->_config->catalog->product->configurable) {
            $filterable = [];
            foreach ($custom as $attributeCode => $attributeData) {
                if (strpos($attributeCode, 'filterable') !== false) {
                    $filterable[$attributeData['attribute_id']] = $attributeData['value'];
                }
            }
            $this->_catalogProductSimpleSuper[$setId][] = [
                $entityId,
                $filterable,
            ];
        }
    }

    /**
     * Populates Catalog Product Attributes cache
     *
     * @param int $entityId
     * @param array $attributes
     * @param array $custom
     */
    protected function _storeCatalogProductAttributes($entityId, $attributes, $custom)
    {
        foreach ($attributes as $attributeCode => $value) {
            if (!isset($this->_catalogProductAttributeCache[$attributeCode])) {
                $this->_catalogProductAttributeCache[$attributeCode] = $this->_eavConfig
                    ->getAttribute($this->_catalogProductEntityTypeId, $attributeCode);
            }
            $attribute      = $this->_catalogProductAttributeCache[$attributeCode];
            $table          = $attribute->getBackendTable();
            $attributeData  = [
                'value_id'       => null,
//                'entity_type_id' => $this->_catalogProductEntityTypeId,
                'attribute_id'   => (int) $attribute->getId(),
                'store_id'       => 0,
                'row_id'      => $entityId,
                'value'          => $value,
            ];

            $this->_bufferCatalogProductAttributes[$table][] = $attributeData;
        }

        foreach ($custom as $attributeCode => $customData) {
            $table          = $customData['table'];
            $attributeData  = [
                'value_id'       => null,
//                'entity_type_id' => $this->_catalogProductEntityTypeId,
                'attribute_id'   => $customData['attribute_id'],
                'store_id'       => 0,
                'row_id'      => $entityId,
                'value'          => $customData['value'],
            ];

            $this->_bufferCatalogProductAttributes[$table][] = $attributeData;
        }
    }

    /**
     * Generates Special, Tier and Group prices
     *
     * @param array $entity
     * @param array $attributes
     * @param \stdClass $priceConfig
     */
    protected function _generateCatalogProductPrices($entity, &$attributes, $priceConfig)
    {
        $entityId = $entity['entity_id'];
        $price    = $attributes['price'];

        // special price
        if ($priceConfig->special && mt_rand(1, 100) < (int) $priceConfig->special) {
            $ratio = mt_rand(60, 95) / 100;
            $attributes['special_price']     = sprintf('%.4F', round($price * $ratio, 2));
            $attributes['special_from_date'] = mt_rand(1, 3) === 2
                ? sprintf('%4d-%02d-%02d', '2015', mt_rand(1, 12), mt_rand(1, 28))
                : null;
            $attributes['special_to_date']   = mt_rand(1, 3) === 2
                ? sprintf('%4d-%02d-%02d', mt_rand(2015, 2017), mt_rand(1, 12), mt_rand(1, 28))
                : null;
        }

        // group price
        if ($priceConfig->group && mt_rand(1, 100) <= (int) $priceConfig->group) {
            foreach ($this->_customerGroups as $groupId) {
                if (mt_rand(1, 100) > (int) $priceConfig->group) {
                    continue;
                }
                $ratio = mt_rand(60, 95) / 100;
                $groupPriceData = [
                    'value_id'          => null,
                    'entity_id'         => $entityId,
                    'all_groups'        => 0,
                    'customer_group_id' => (int)$groupId,
                    'value'             => sprintf('%.4F', round($price * $ratio, 2)),
                    'website_id'        => 0,

                ];
                $this->_bufferCatalogProductPriceGroup[] = $groupPriceData;
            }
        }

        // tier price
        if ($priceConfig->tier && mt_rand(1, 100) <= (int) $priceConfig->tier) {
            foreach ([2, 5, 10] as $qty) {
                foreach (array_merge([0], $this->_customerGroups) as $groupId) {
                    if ((int) $priceConfig->tier === 100 && $groupId === 0) {
                        continue;
                    }
                    if (mt_rand(1, 100) > $priceConfig->tier) {
                        continue;
                    }
                    $ratio          = (100 - mt_rand($qty, $qty + 3)) / 100;
                    $tierPriceData  = [
                        'value_id'          => null,
                        'row_id'         => $entityId,
                        'all_groups'        => $groupId > -1 ? 0 : 1,
                        'customer_group_id' => (int) $groupId,
                        'qty'               => $qty,
                        'value'             => sprintf('%.4F', round($price * $ratio, 2)),
                        'website_id'        => 0,
                    ];
                    $this->_bufferCatalogProductPriceTier[] = $tierPriceData;

                    // copy general to not logged in
                    if ((int) $groupId === 1) {
                        $tierPriceData['customer_group_id'] = 0;
                        $this->_bufferCatalogProductPriceTier[] = $tierPriceData;
                    }

                    if ((int) $groupId === 0) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * Generates CatalogInventory Stock Item
     *
     * @param int $entityId
     * @param \stdClass $productConfig
     */
    protected function _generateCatalogInventoryStockItem($entityId, $productConfig)
    {
        $qty       = isset($productConfig->inventory)
            ? mt_rand($productConfig->inventory->min, $productConfig->inventory->max)
            : 0;
        $stockItem = [
            'item_id'                     => null,
            'product_id'                  => $entityId,
            'stock_id'                    => 1,
            'qty'                         => $qty,
            'min_qty'                     => 0,
            'use_config_min_qty'          => 1,
            'is_qty_decimal'              => 0,
            'backorders'                  => 0,
            'use_config_backorders'       => 1,
            'min_sale_qty'                => 1,
            'use_config_min_sale_qty'     => 1,
            'max_sale_qty'                => 0,
            'use_config_max_sale_qty'     => 1,
            'is_in_stock'                 => mt_rand(1, 100) > $productConfig->out_of_stock ? 1 : 0,
            'low_stock_date'              => null,
            'notify_stock_qty'            => null,
            'use_config_notify_stock_qty' => 1,
            'manage_stock'                => 1,
            'use_config_manage_stock'     => 1,
            'stock_status_changed_auto'   => 0,
            'use_config_qty_increments'   => 1,
            'qty_increments'              => 0.0000,
            'use_config_enable_qty_inc'   => 1,
            'enable_qty_increments'       => 0,
            'is_decimal_divided'          => 0,
            'website_id'                  => $this->_websiteId,
        ];

        $this->_bufferCatalogInventoryStockItem[] = $stockItem;
    }

    /**
     * Renders Catalog Product Custom Attribute data set
     *
     * @param int $setId
     * @param bool $filterable
     * @return array
     */
    protected function _getCatalogProductCustomAttributes($setId, $filterable = true)
    {
        $result = [];
        foreach ($this->_catalogProductAttributeSetMap[$setId] as $attributeId) {
            $attributeCode = $this->_catalogProductAttributeMap[$attributeId];
            $attribute     = $this->_catalogProductAttributes[$attributeCode];
            if (!$filterable && $attribute['is_filterable']) {
                continue;
            }
            $value         = null;
            $table         = $this->_getTable(['catalog_product_entity', $attribute['backend_type']]);
            switch ($attribute['frontend_input']) {
                case 'select':
                    $value = $attribute['options'][array_rand($attribute['options'], 1)];
                    break;
                case 'boolean':
                    $value = mt_rand(0, 1);
                    break;
                case 'text':
                    $value = $this->_renderTextAttributeValue();
                    break;
                case 'textarea':
                    $value = $this->_renderTextAreaAttributeValue();
                    break;
            }

            $result[$attributeCode] = [
                'table'        => $table,
                'attribute_id' => $attributeId,
                'value'        => $value,
            ];
        }

        return $result;
    }

    /**
     * Generates Catalog Product Image Data and populates symlink file
     *
     * @param array $entity
     * @param array $attributes
     */
    protected function _generateCatalogProductImages(array $entity, array &$attributes)
    {
        $imageConfig = $this->_config->catalog->product->image;
        if (!$imageConfig) {
            return;
        }

        if ($this->_catalogProductGalleryAttributeId === null) {
            $this->_catalogProductGalleryAttributeId = (int)$this->_eavConfig
                ->getAttribute('catalog_product', 'media_gallery')->getId();
        }

        $count   = mt_rand($imageConfig->min, $imageConfig->max);
        $default = false;

        list(, $idx, $rnd) = explode('-', $entity['sku']);

        $imageTpl = sprintf('%s_%s_%%02d.jpg', strtolower($rnd), $idx);
        for ($i = 1; $i <= $count; $i ++) {
            $imageFile = sprintf($imageTpl, $i);
            $imagePath = sprintf('/%s/%s/%s', $imageFile[0], $imageFile[1], $imageFile);
            if ($default === false) {
                $default = $imagePath;
            }

            $valueId            = ++ $this->_maxCatalogProductMediaGallery;
            $mediaGalleryData   = [
                'value_id'     => $valueId,
                'attribute_id' => $this->_catalogProductGalleryAttributeId,
                //'entity_id'    => $entity['entity_id'],
                'value'        => $imagePath,
                'media_type'    => 'image',
                'disabled' => 0,
            ];
            $mediaValueData     = [
                'value_id' => $valueId,
                'store_id' => 0,
                'row_id' => $entity['entity_id'],
                'label'    => null,
                'position' => $i,
                'disabled' => 0,
            ];

            $mediaValueToEntityData = [
                'value_id' => $valueId,
                'row_id' => $entity['entity_id']
            ];

            $this->_bufferCatalogProductMediaGallery[]  = $mediaGalleryData;
            $this->_bufferCatalogProductMediaValue[]    = $mediaValueData;
            $this->_bufferCatalogProductMediaValueToEntity[] = $mediaValueToEntityData;

            $this->_saveImage($imagePath . PHP_EOL);
        }

        if ($default) {
            $attributes['image']       = $default;
            $attributes['small_image'] = $default;
            $attributes['thumbnail']   = $default;
        }
    }

    /**
     * Generates Catalog Product to Category relations
     *
     * @param int $entityId
     * @param array $categories
     * @param string $sku
     */
    protected function _generateCatalogProductCategories($entityId, $categories, $sku)
    {
        $min = $this->_config->catalog->product->category->min;
        $max = $this->_config->catalog->product->category->max;
        $cnt = min(mt_rand($min, $max), count($categories));
        $rnd = (array)array_rand($categories, $cnt);

        $urlRequest = sprintf('%s%s', $sku, $this->_catalogProductSuffix);
        $urlTarget  = sprintf('catalog/product/view/id/%s', $entityId);

        $data    = [
            'redirect_type'    => 0,
            'is_autogenerated' => 1,
            'metadata'         => null,
            'description'      => null,
            'store_id'         => $this->_storeId,
            'entity_type'      => 'product',
            'entity_id'        => $entityId,
            'request_path'     => $urlRequest,
            'target_path'      => $urlTarget,
        ];

        $this->_bufferUrlRewrite[] = $data;

        foreach ($rnd as $i) {
            $category = [
                'category_id' => (int) $categories[$i],
                'product_id'  => $entityId,
                'position'    => 0,
            ];

            $this->_bufferCatalogProductCategory[] = $category;

            if (array_key_exists($categories[$i], $this->_urlRewrites)) {
                $catUrl     = $this->_urlRewrites[$categories[$i]];
                $catData    = serialize(['category_id' => $categories[$i]]);
                $urlRequest = sprintf('%s/%s%s', $catUrl, $sku, $this->_catalogProductSuffix);
                $urlTarget  = sprintf('catalog/product/view/id/%d/category/%d', $entityId, $categories[$i]);
                $data   = [
                    'redirect_type'    => 0,
                    'is_autogenerated' => 1,
                    'metadata'         => $catData,
                    'description'      => null,
                    'store_id'         => $this->_storeId,
                    'entity_type'      => 'product',
                    'entity_id'        => $entityId,
                    'request_path'     => $urlRequest,
                    'target_path'      => $urlTarget,
                ];

                $this->_bufferUrlRewrite[] = $data;
            }
        }
    }

    /**
     * Generates catalog grouped products
     */
    protected function _prepareCatalogProductGrouped()
    {
        if (!property_exists($this->_config->catalog->product, 'grouped')) {
            return;
        }

        $timer  = microtime(true);
        $start  = $timer;
        $this->_output->write('Starting to generate catalog grouped products' . PHP_EOL);

        $count = $this->_config->catalog->product->grouped->count;
        for ($i = 1; $i <= $count; $i ++) {
            $this->_generateCatalogProductGrouped(null, $i);
            if ($i % 500 === 0) {
                $this->_flushCatalogProduct();
            }
            if ($i % 1000 === 0) {
                $finish = microtime(true);
                $this->_output->write([
                    sprintf("\t" . 'Generated %08d grouped products in %s', $i, $this->_formatTime($finish - $start)),
                    PHP_EOL,
                ]);
                $start  = $finish;
            }
        }

        $this->_flushCatalogProduct();

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Catalog grouped products have been generated in %s', $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Generates Catalog Grouped Product
     *
     * @param array|null $topCategory
     * @param int $index
     */
    protected function _generateCatalogProductGrouped(array $topCategory = null, $index)
    {
        if ($topCategory === null) {
            $rootId      = array_rand($this->_catalogCategories, 1);
            $topId       = array_rand($this->_catalogCategories[$rootId], 1);
            $topCategory = $this->_catalogCategories[$rootId][$topId];
        }

        $deep           = $this->_config->catalog->category->deep;
        if(!isset($topCategory[$deep])){
            return;
        }
        $categories     = $topCategory[$deep];
        $setId          = $topCategory['set_id'];
        $productConfig  = $this->_config->catalog->product->grouped;

        if ($productConfig->start) {
            $index = $productConfig->start + $index;
        }

        $entityId       = ++ $this->_maxCatalogProductEntityId;
        $entity         = [
            'entity_id'        => $entityId,
//            'entity_type_id'   => $this->_catalogProductEntityTypeId,
            'attribute_set_id' => $setId,
            'type_id'          => 'grouped',
            'sku'              => $this->_renderCatalogProductSku($index),
            'has_options'      => 0,
            'required_options' => 0,
            'created_at'       => '2015-01-01 00:00:00',
            'updated_at'       => '2015-01-01 00:00:00',
        ];

        if ($this->_catalogProductLinkAttributes === null) {
            $this->_catalogProductLinkAttributes = [];
            $select = $this->_adapter->select()
                ->from($this->_getTable('catalog_product_link_attribute'))
                ->where('link_type_id = :link_type_id');
            $bind   = [
                ':link_type_id' => \Magento\GroupedProduct\Model\Resource\Product\Link::LINK_TYPE_GROUPED,
            ];
            foreach ($this->_adapter->fetchAll($select, $bind) as $row) {
                $this->_catalogProductLinkAttributes[$row['product_link_attribute_code']] = [
                    'link_attribute_id' => $row['product_link_attribute_id'],
                    'data_type'         => $row['data_type'],
                ];
            }
        }

        $this->_bufferCatalogProductEntity[] = $entity;
        $this->_bufferCatalogProductEntitySequence[] = ['sequence_value' => $entityId];

        $associated = [];
        $count      = mt_rand($productConfig->associated->min, $productConfig->associated->max);
        for ($i = 1; $i <= $count; $i ++) {
            $simpleCount = count($this->_catalogProductSimple);
            $simpleIdx   = mt_rand(0, $simpleCount - 1);
            $childId     = $this->_catalogProductSimple[$simpleIdx];
            if (in_array($childId, $associated)) {
                $i --;
                continue;
            }

            $associated[] = $childId;
            $relationData = [
                'parent_id' => $entityId,
                'child_id'  => $childId,
            ];

            $this->_bufferCatalogProductRelation[] = $relationData;

            $linkId       = ++ $this->_maxCatalogProductLinkId;
            $linkData     = [
                'link_id'           => $linkId,
                'product_id'        => $entityId,
                'linked_product_id' => $childId,
                'link_type_id'      => (int) \Magento\GroupedProduct\Model\Resource\Product\Link::LINK_TYPE_GROUPED,
            ];

            $linkTable  = $this->_getTable('catalog_product_link');
            $this->_bufferCatalogProductLink[$linkTable][] = $linkData;

            // position
            if (isset($this->_catalogProductLinkAttributes['position'])) {
                $linkAttributeId    = $this->_catalogProductLinkAttributes['position']['link_attribute_id'];
                $linkAttributeTable = $this->_getTable([
                    'catalog_product_link_attribute',
                    $this->_catalogProductLinkAttributes['position']['data_type']
                ]);
                $linkAttributeData = [
                    'value_id'                  => null,
                    'product_link_attribute_id' => $linkAttributeId,
                    'link_id'                   => $linkId,
                    'value'                     => $i,
                ];
                $this->_bufferCatalogProductLink[$linkAttributeTable][] = $linkAttributeData;
            }

            // qty
            if (isset($this->_catalogProductLinkAttributes['qty'])) {
                $qty                = sprintf('%.4F', mt_rand(0, 1));
                $linkAttributeId    = $this->_catalogProductLinkAttributes['qty']['link_attribute_id'];
                $linkAttributeTable = $this->_getTable([
                    'catalog_product_link_attribute',
                    $this->_catalogProductLinkAttributes['qty']['data_type']
                ]);
                $linkAttributeData = [
                    'value_id'                  => null,
                    'product_link_attribute_id' => $linkAttributeId,
                    'link_id'                   => $linkId,
                    'value'                     => $qty,
                ];
                $this->_bufferCatalogProductLink[$linkAttributeTable][] = $linkAttributeData;
            }
        }

        $status         = mt_rand(1, 100) > $productConfig->disabled ? 1 : 0;
        $attributes = [
            'name'              => $this->_getRandomWords(1, 3, 10),
            'url_key'           => strtolower($entity['sku']),
            'page_layout'       => null,
            'options_container' => 'container1',
            'status'            => $status,
            'visibility'        => 4,
            'tax_class_id'      => 0,
            'description'       => $this->_getRandomWords(10, 30, 15),
            'short_description' => $this->_getRandomWords(2, 10, 10),
            'quantity_and_stock_status' => 1,
        ];

        $this->_generateCatalogProductImages($entity, $attributes);

        $custom = $this->_getCatalogProductCustomAttributes($setId);
        $this->_storeCatalogProductAttributes($entityId, $attributes, $custom);

        $this->_generateCatalogInventoryStockItem($entityId, $productConfig);
        $this->_generateCatalogProductCategories($entityId, $categories, strtolower($entity['sku']));

        $website = [
            'product_id' => $entityId,
            'website_id' => $this->_websiteId,
        ];

        $this->_bufferCatalogProductWebsite[] = $website;
    }

    /**
     * Generates catalog configurable products
     */
    protected function _prepareCatalogProductConfigurable()
    {
        if (!$this->_config->catalog->product->configurable) {
            return;
        }

        $timer  = microtime(true);
        $start  = $timer;
        $this->_output->write('Starting to generate catalog configurable products' . PHP_EOL);

        $count = $this->_config->catalog->product->configurable->count;
        for ($i = 1; $i <= $count; $i ++) {
            $this->_generateCatalogProductConfigurable(null, $i);
            if ($i % 500 == 0) {
                $this->_flushCatalogProduct();
            }
            if ($i % 1000 == 0) {
                $finish = microtime(true);
                $this->_output->write([
                    sprintf("\t" . 'Generated %08d configurable products in %s', $i,
                        $this->_formatTime($finish - $start)),
                    PHP_EOL,
                ]);
                $start  = $finish;
            }
        }

        $this->_flushCatalogProduct();

        $finish = microtime(true);
        $this->_output->write([
            sprintf('Catalog configurable products have been generated in %s', $this->_formatTime($finish - $timer)),
            PHP_EOL,
        ]);
    }

    /**
     * Generates Catalog Configurable Product
     *
     * @param array|null $topCategory
     * @param int $index
     */
    protected function _generateCatalogProductConfigurable(array $topCategory = null, $index)
    {
        if ($topCategory === null) {
            $rootId      = array_rand($this->_catalogCategories, 1);
            $topId       = array_rand($this->_catalogCategories[$rootId], 1);
            $topCategory = $this->_catalogCategories[$rootId][$topId];
        }

        $deep           = $this->_config->catalog->category->deep;
        if(!isset($topCategory[$deep])){
            return;
        }
        $categories     = $topCategory[$deep];
        $setId          = $topCategory['set_id'];
        $productConfig  = $this->_config->catalog->product->configurable;

        if ($productConfig->start) {
            $index = $productConfig->start + $index;
        }

        $entityId       = ++ $this->_maxCatalogProductEntityId;
        $entity         = [
            'entity_id'        => $entityId,
            'created_in'       => 1,
            'updated_in'       => time(),
//            'entity_type_id'   => $this->_catalogProductEntityTypeId,
            'attribute_set_id' => $setId,
            'type_id'          => 'configurable',
            'sku'              => $this->_renderCatalogProductSku($index),
            'has_options'      => 1,
            'required_options' => 1,
            'created_at'       => '2015-01-01 00:00:00',
            'updated_at'       => '2015-01-01 00:00:00',
        ];

        $this->_bufferCatalogProductEntity[] = $entity;
        $this->_bufferCatalogProductEntitySequence[] = ['sequence_value' => $entityId];

        $super      = [];
        $count      = mt_rand($productConfig->attribute->min, $productConfig->attribute->max);
        for ($i = 1; $i <= $count; $i ++) {
            $attributeIdx = mt_rand(0, count($this->_catalogProductAttributeSetMap[$setId]) - 1);
            $attributeId  = $this->_catalogProductAttributeSetMap[$setId][$attributeIdx];
            $attributeCode = $this->_catalogProductAttributeMap[$attributeId];
            $attribute     = $this->_catalogProductAttributes[$attributeCode];
            if (!$attribute['is_filterable'] || isset($super[$attributeId])) {
                $i --;
                continue;
            }
            $super[$attributeId] = $attribute;


            $superAttributeId    = ++ $this->_maxCatalogProductSuperAttributeId;
            $superAttributeTable = $this->_getTable('catalog_product_super_attribute');
            $superAttributeData  = [
                'product_super_attribute_id' => $superAttributeId,
                'product_id'                 => $entityId,
                'attribute_id'               => $attributeId,
                'position'                   => 0,
            ];

            $this->_bufferCatalogProductSuper[$superAttributeTable][] = $superAttributeData;

            $superLabelTable     = $this->_getTable('catalog_product_super_attribute_label');
            $superLabelData      = [
                'value_id'                   => null,
                'product_super_attribute_id' => $superAttributeId,
                'store_id'                   => 0,
                'use_default'                => 1,
                'value'                      => $attribute['frontend_label'],
            ];

            $this->_bufferCatalogProductSuper[$superLabelTable][] = $superLabelData;
        }

        $associated = [];
        $count      = mt_rand($productConfig->associated->min, $productConfig->associated->max);
        for ($i = 1; $i <= $count; $i ++) {
            $simpleCount = count($this->_catalogProductSimpleSuper[$setId]);
            $simpleIdx   = mt_rand(0, $simpleCount - 1);
            $childId     = $this->_catalogProductSimpleSuper[$setId][$simpleIdx][0];

            if (isset($associated[$childId])) {
                $i --;
                continue;
            }

            $superKey    = [];
            $childData   = $this->_catalogProductSimpleSuper[$setId][$simpleIdx][1];
            // validate
            foreach ($super as $superId => $superData) {
                $superKey[] = $childData[$superId];
            }
            $superKey = implode('-', $superKey);

            if (in_array($superKey, $associated)) {
                $i --;
                continue;
            }

            $associated[$childId] = $superKey;

            $relationData = [
                'parent_id' => $entityId,
                'child_id'  => $childId,
            ];

            $this->_bufferCatalogProductRelation[] = $relationData;

            $linkTable = $this->_getTable('catalog_product_super_link');
            $linkData  = [
                'link_id'    => null,
                'product_id' => $childId,
                'parent_id'  => $entityId,
            ];

            $this->_bufferCatalogProductSuper[$linkTable][] = $linkData;
        }

        $priceConfig    = $productConfig->price;
        $price          = sprintf('%.4F', mt_rand($priceConfig->min * 100, $priceConfig->max * 100) / 100);
        $status         = mt_rand(1, 100) > $productConfig->disabled ? 1 : 0;
        $attributes = [
            'name'              => $this->_getRandomWords(1, 3, 10),
            'url_key'           => strtolower($entity['sku']),
            'price'             => $price,
            'page_layout'       => null,
            'options_container' => 'container1',
            'status'            => $status,
            'visibility'        => 4,
            'tax_class_id'      => 0,
            'description'       => $this->_getRandomWords(10, 30, 15),
            'short_description' => $this->_getRandomWords(2, 10, 10),
            'quantity_and_stock_status' => 1,
        ];

        $this->_generateCatalogProductImages($entity, $attributes);
        $custom = $this->_getCatalogProductCustomAttributes($setId, false);
        $this->_storeCatalogProductAttributes($entityId, $attributes, $custom);

        $this->_generateCatalogInventoryStockItem($entityId, $productConfig);
        $this->_generateCatalogProductCategories($entityId, $categories, strtolower($entity['sku']));

        $website = [
            'product_id' => $entityId,
            'website_id' => $this->_websiteId,
        ];

        $this->_bufferCatalogProductWebsite[] = $website;
    }

    /**
     * Flushes Catalog Product Data to output
     */
    protected function _flushCatalogProduct()
    {
        if ($this->_bufferCatalogProductEntity) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_entity'),
                $this->_bufferCatalogProductEntity));
            $this->_bufferCatalogProductEntity = [];
        }

        if ($this->_bufferCatalogProductEntitySequence) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('sequence_product'),
                $this->_bufferCatalogProductEntitySequence));
            $this->_bufferCatalogProductEntitySequence = [];
        }

        if ($this->_bufferCatalogProductAttributes) {
            foreach ($this->_bufferCatalogProductAttributes as $table => $data) {
                $this->_saveData($this->_getInsertMultiplySql($table, $data));
                unset($this->_bufferCatalogProductAttributes[$table]);
            }
        }

        if ($this->_bufferCatalogProductCategory) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_category_product'),
                $this->_bufferCatalogProductCategory));
            $this->_bufferCatalogProductCategory = [];
        }

        if ($this->_bufferCatalogProductWebsite) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_website'),
                $this->_bufferCatalogProductWebsite));
            $this->_bufferCatalogProductWebsite = [];
        }

        // disable
        /*if ($this->_bufferCatalogProductPriceGroup) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_entity_group_price'),
                $this->_bufferCatalogProductPriceGroup));
            $this->_bufferCatalogProductPriceGroup = [];
        }*/

        if ($this->_bufferCatalogProductPriceTier) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_entity_tier_price'),
                $this->_bufferCatalogProductPriceTier));
            $this->_bufferCatalogProductPriceTier = [];
        }

        if ($this->_bufferCatalogProductMediaGallery) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_entity_media_gallery'),
                $this->_bufferCatalogProductMediaGallery));
            $this->_bufferCatalogProductMediaGallery = [];
        }

        if ($this->_bufferCatalogProductMediaValue) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_entity_media_gallery_value'),
                $this->_bufferCatalogProductMediaValue));
            $this->_bufferCatalogProductMediaValue = [];
        }

        if ($this->_bufferCatalogProductMediaValueToEntity) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_entity_media_gallery_value_to_entity'),
                $this->_bufferCatalogProductMediaValueToEntity));
            $this->_bufferCatalogProductMediaValueToEntity = [];
        }

        if ($this->_bufferCatalogInventoryStockItem) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('cataloginventory_stock_item'),
                $this->_bufferCatalogInventoryStockItem));
            $this->_bufferCatalogInventoryStockItem = [];
        }

        if ($this->_bufferCatalogProductRelation) {
            $this->_saveData($this->_getInsertMultiplySql($this->_getTable('catalog_product_relation'),
                $this->_bufferCatalogProductRelation));
            $this->_bufferCatalogProductRelation = [];
        }

        if ($this->_bufferCatalogProductLink) {
            foreach ($this->_bufferCatalogProductLink as $table => $data) {
                $this->_saveData($this->_getInsertMultiplySql($table, $data));
                unset($this->_bufferCatalogProductLink[$table]);
            }
        }

        if ($this->_bufferCatalogProductSuper) {
            foreach ($this->_bufferCatalogProductSuper as $table => $data) {
                $this->_saveData($this->_getInsertMultiplySql($table, $data));
                unset($this->_bufferCatalogProductSuper[$table]);
            }
        }

        $this->_flushUrlRewrite();
    }
}
