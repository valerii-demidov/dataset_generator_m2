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
use Magento\ConfigurableProduct\Model\Product\Type\Configurable as ConfigurableProductType;
use Magento\Catalog\Model\Product\Type as ProductType;


/**
 * Command for executing dataset generation
 */
class ExportGatlingData extends Command
{
    const EXPORT_PRODUCT_SIMPLE = 'importExport/Gatling/product_simple.csv';
    const EXPORT_PRODUCT_CONFIGURABLE = 'importExport/Gatling/product_configurable.csv';
    const EXPORT_LAYER = 'importExport/Gatling/layer.csv';
    const EXPORT_CATALOG_SEARCH = 'importExport/Gatling/catalog_search.csv';

    const CATEGORY_PARENT_ID = 2;


    const DEFAULT_GLOBAL_MULTI_VALUE_SEPARATOR = '&';

    private $useCollectionLimit = true;
    private $collectionLimitSize = 100;

    private $objectManager;
    private $catalogProductCollection;

    // for layered navigation
    private $filterableAttributes;
    private $appState;
    private $layerResolverFactory;
    private $categoryCollectionFactory;

    private $productConfigurableHeader = ['product_id', 'url', 'options'];
    private $productSimpleHeader = ['product_id', 'url'];
    private $catalogSearchHeader = ['sku', 'product_name','product_short_description'];
    private $layeredHeader = ['category_id', 'url', 'count', 'attribute', 'option'];

    /**
     * ExportCategory constructor.
     * @param ObjectManagerInterface $objectManager
     */
    public function __construct(
        ObjectManagerInterface $objectManager,
        \Magento\Catalog\Model\ResourceModel\Product\CollectionFactory $catalogProductCollection,
        \Magento\Catalog\Model\Layer\Category\FilterableAttributeList $filterableAttributeList,
        \Magento\Framework\App\State $appSate,
        \Magento\Catalog\Model\Layer\ResolverFactory $layerResolverFactory,
        \Magento\Catalog\Model\ResourceModel\Category\CollectionFactory $categoryCollectionFactory
    ) {
        $this->objectManager = $objectManager;
        $this->catalogProductCollection = $catalogProductCollection;

        $this->filterableAttributes = $filterableAttributeList;
        $this->appState = $appSate;
        $this->layerResolverFactory = $layerResolverFactory;
        $this->categoryCollectionFactory = $categoryCollectionFactory;

        parent::__construct();
    }

    protected function configure()
    {
        $this->setName('exportgatlingdata:run')
            ->setDescription('Run Export Of Gatling Data');


        parent::configure();
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        //$this->exportConfigurableProducts();
        //$this->exportSimpleProducts();
        //$this->exportLayeredFilters();
        $this->exportCatalogSearch();
    }

    protected function exportCatalogSearch()
    {
        $path = static::EXPORT_CATALOG_SEARCH;
        echo 'writing data to '.$path.PHP_EOL;
        $this->prepareDir($path);

        //prepare collection
        $catalogProductCollection = $this->catalogProductCollection
            ->create()
            ->addAttributeToSelect('sku')
            ->addAttributeToSelect('name')
            ->addAttributeToSelect('short_description')
            ->addAttributeToFilter('status', \Magento\Catalog\Model\Product\Attribute\Source\Status::STATUS_ENABLED)
            ->joinField(
                'is_in_stock',
                'cataloginventory_stock_item',
                'is_in_stock',
                'product_id=entity_id',
                'is_in_stock=1',
                '{{table}}.stock_id=1'
            )
            ->addAttributeToFilter('type_id', ['eq' => ProductType::TYPE_SIMPLE]);

        //set limit
        if ($this->useCollectionLimit) {
            $catalogProductCollection
                ->setPageSize($this->collectionLimitSize)
                ->setCurPage(1);
        }

        //prepare collection
        $catalogProductConfigurableCollection = $this->catalogProductCollection
            ->create()
            ->addAttributeToSelect('sku')
            ->addAttributeToSelect('name')
            ->addAttributeToSelect('short_description')
            ->addAttributeToFilter('status', \Magento\Catalog\Model\Product\Attribute\Source\Status::STATUS_ENABLED)
            ->joinField(
                'is_in_stock',
                'cataloginventory_stock_item',
                'is_in_stock',
                'product_id=entity_id',
                'is_in_stock=1',
                '{{table}}.stock_id=1'
            )
            ->addAttributeToFilter('type_id', ['eq' => ConfigurableProductType::TYPE_CODE]);

        //set limit
        if ($this->useCollectionLimit) {
            $catalogProductConfigurableCollection
                ->setPageSize($this->collectionLimitSize)
                ->setCurPage(1);
        }


        $file = fopen($path, 'w');
        $this->prepareHeader($file, $this->catalogSearchHeader);
        foreach ($catalogProductCollection as $product) {
            $data = [
                'sku' => $product->getSku(),
                'product_name' => $product->getName(),
                'product_short_description' => $product->getShortDescription(),
            ];
            fputcsv($file, $data);
        }

        foreach ($catalogProductConfigurableCollection as $product) {
            $data = [
                'sku' => $product->getSku(),
                'product_name' => $product->getName(),
                'product_short_description' => $product->getShortDescription(),
            ];
            fputcsv($file, $data);
        }
        fclose($file);
    }

    protected function exportLayeredFilters()
    {
        $path = static::EXPORT_LAYER;
        echo 'writing data to '.$path.PHP_EOL;
        $this->prepareDir($path);

        //initial data
        $this->appState->setAreaCode('frontend');
        $rootCategories = $this->categoryCollectionFactory
            ->create()
            ->addAttributeToSelect('url_path')
            ->addAttributeToFilter(
                'parent_id',
                static::CATEGORY_PARENT_ID
            );

        $file = fopen($path, 'w');
        $this->prepareHeader($file, $this->layeredHeader);
        foreach ($rootCategories as $category) {
            $data = [
                'category_id' => $category->getId(),
                'url' => $category->getUrlPath().".html",
            ];

            $filterList = $this->objectManager->create(
                \Magento\Catalog\Model\Layer\FilterList::class,
                [
                    'filterableAttributes' => $this->filterableAttributes,
                ]
            );
            $layerResolver = $this->layerResolverFactory->create();

            $layer = $layerResolver->get();
            $layer->setCurrentCategory($category->getId());
            $filters = $filterList->getFilters($layer);
            foreach ($filters as $filter) {
                if ($filter instanceof \Magento\Catalog\Model\Layer\FIlter\Attribute) {
                    $items = $filter->getItems();
                    if (count($items)) {
                        foreach ($items as $item) {
                            $data['count'] = $item->getCount();
                            $data['attribute'] = $filter->getAttributeModel()->getAttributeCode();
                            $data['option'] = $item->getValue();
                            fputcsv($file, $data);
                        }
                    }
                }
            }
        }
        fclose($file);
    }

    protected function exportSimpleProducts()
    {
        $path = static::EXPORT_PRODUCT_SIMPLE;
        echo 'writing data to '.$path.PHP_EOL;
        $this->prepareDir($path);

        //prepare collection
        $catalogProductCollection = $this->catalogProductCollection
            ->create()
            ->addAttributeToSelect('url_key')
            ->addAttributeToFilter('status', \Magento\Catalog\Model\Product\Attribute\Source\Status::STATUS_ENABLED)
            ->joinField(
                'is_in_stock',
                'cataloginventory_stock_item',
                'is_in_stock',
                'product_id=entity_id',
                'is_in_stock=1',
                '{{table}}.stock_id=1'
            )
        ->addAttributeToFilter('type_id', ['eq' => ProductType::TYPE_SIMPLE]);

        //set limit
        if ($this->useCollectionLimit) {
            $catalogProductCollection
                ->setPageSize($this->collectionLimitSize)
                ->setCurPage(1);
        }

        $file = fopen($path, 'w');
        $this->prepareHeader($file, $this->productSimpleHeader);
        foreach ($catalogProductCollection as $product) {
            $data = [
                'product_id' => $product->getId(),
                'url' => $product->getUrlKey().'.html',
            ];
            fputcsv($file, $data);
        }
        fclose($file);
    }

    protected function exportConfigurableProducts()
    {
        $path = static::EXPORT_PRODUCT_CONFIGURABLE;
        echo 'writing data to '.$path.PHP_EOL;
        $this->prepareDir($path);

        //prepare collection
        $catalogProductCollection = $this->catalogProductCollection
            ->create()
            ->addAttributeToSelect('url_key')
            ->addAttributeToFilter('status', \Magento\Catalog\Model\Product\Attribute\Source\Status::STATUS_ENABLED)
            ->joinField(
                'is_in_stock',
                'cataloginventory_stock_item',
                'is_in_stock',
                'product_id=entity_id',
                'is_in_stock=1',
                '{{table}}.stock_id=1'
            )
            ->addAttributeToFilter('type_id', ['eq' => ConfigurableProductType::TYPE_CODE]);

        //set limit
        if ($this->useCollectionLimit) {
            $catalogProductCollection
                ->setPageSize($this->collectionLimitSize)
                ->setCurPage(1);
        }

        $file = fopen($path, 'w');
        $this->prepareHeader($file, $this->productConfigurableHeader);
        foreach ($catalogProductCollection as $product) {
            $this->exportConfigurableData($product, $file);
        }
        fclose($file);
    }

    /**
     * @param \Magento\Catalog\Model\Product $product
     * @param $file
     */
    protected function exportConfigurableData(\Magento\Catalog\Model\Product $product, $file)
    {
        $data = [
            'product_id' => $product->getId(),
            'url' => $product->getUrlKey().'.html',
        ];

        $productConfigurableVariants = [];
        $configurableOptions = $product->getTypeInstance()->getConfigurableOptions($product);
        foreach ($configurableOptions as $attributeId => $options) {
            foreach ($options as $optionData) {
                $productConfigurableVariants[$optionData['sku']][] = sprintf(
                    "%s=%s",
                    $attributeId,
                    $optionData['value_index']
                );
            }
        }

        foreach ($productConfigurableVariants as $updatedOption) {
            if (count($updatedOption) > 1) {
                $data['options'] = implode(static::DEFAULT_GLOBAL_MULTI_VALUE_SEPARATOR, $updatedOption);
                fputcsv($file, $data);
            }
        }
    }

    /**
     * @param $file
     * @param array $header
     */
    protected function prepareHeader($file, $header = [])
    {
        fputcsv($file, $header);
    }

    protected function prepareDir($filePath)
    {
        $dirname = dirname($filePath);
        if (!is_dir($dirname)) {
            mkdir($dirname, 0755, true);
        }
    }
}
