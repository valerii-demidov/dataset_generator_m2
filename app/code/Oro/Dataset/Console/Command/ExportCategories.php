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
class ExportCategories extends Command
{
    const EXPORT_FILE_NAME = 'export_categories.csv';
    const CATEGORY_PARENT_ID = 2;

    private $objectManager;
    private $categoryCollectionFactory;
    private $categoryFactory;

    /**
     * ExportCategory constructor.
     * @param ObjectManagerInterface $objectManager
     */
    public function __construct(
        ObjectManagerInterface $objectManager
    ) {
        $this->objectManager = $objectManager;
        $this->categoryCollectionFactory = $this->objectManager->get(
            'Magento\Catalog\Model\ResourceModel\Category\CollectionFactory'
        );
        $this->categoryFactory = $this->objectManager->get(
            'Magento\Catalog\Model\CategoryFactory'
        );

        parent::__construct();
    }

    protected function configure()
    {
        $this->setName('exportcategories:run')
            ->setDescription('Run Export Of Categories');


        parent::configure();
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $rootCategoryIds = $this->categoryCollectionFactory->create()->addAttributeToFilter(
            'parent_id',
            static::CATEGORY_PARENT_ID
        )->getAllIds();

        if (false === count($rootCategoryIds)) {
            die ("Please provide at least one root category id. (E.g. --roots 1,3,595)");
        }

        $path = static::EXPORT_FILE_NAME;
        echo 'writing data to '.$path.PHP_EOL;
        $file = fopen($path, 'w');
        $this->prepareHeader($file);
        foreach ($rootCategoryIds as $categoryId) {
            $category = $this->categoryFactory->create()->load($categoryId);
            echo '* '.$category->getName().sprintf(' (%s products)', $category->getProductCount()).PHP_EOL;
            $this->exportData($category, $file);
        }
        fclose($file);

    }

    protected function prepareHeader($file)
    {
        $data = array(
            'id' => 'Category Id',
            'parent_id' => 'Parent Id',
            //'attribute_set_id' => $category->getAttributeSetId(),
            'urlPath' => 'Url Path',
            'urlKey' => 'Url Key',
            'path' => 'Path',
            'position' => "Position",
            //'page_layout'      => '',
            'description' => 'Description',
            //'display_mode'     => $category->getDisplayMode(),
            'is_active' => 'Is Active',
            'is_anchor' => 'Is Anchor',
            'include_in_menu' => 'Include In Menu',
            //'custom_design'    => $category->getCustomDesign(),
            'level' => 'Level',
            'name' => 'Name',
            //'metaTitle'        => $category->getMetaTitle(),
            //'metaKeywords'     => $category->getMetaKeywords(),
            //'metaDescription'  => $category->getMetaDescription(),
        );

        fputcsv($file, $data);
    }

    /**
     * @param \Magento\Catalog\Model\Category $category
     * @param $file
     * @param int $depth
     */
    protected function exportData(\Magento\Catalog\Model\Category $category, $file, $depth = 0)
    {
        $data = array(
            'id' => $category->getId(),
            'parent_id' => $category->getParentId(),
            //'attribute_set_id' => $category->getAttributeSetId(),
            'urlPath' => $category->getUrlPath(),
            'urlKey' => $category->getUrlKey(),
            'path' => $category->getPath(),
            'position' => $category->getPosition(),
            //'page_layout'      => $category->getPageLayout(),
            'description' => $category->getDescription(),
            //'display_mode'     => $category->getDisplayMode(),
            'is_active' => $category->getIsActive(),
            'is_anchor' => $category->getIsAnchor(),
            'include_in_menu' => $category->getIncludeInMenu(),
            //'custom_design'    => $category->getCustomDesign(),
            'level' => $category->getLevel(),
            'name' => $category->getName(),
            //'metaTitle'        => $category->getMetaTitle(),
            //'metaKeywords'     => $category->getMetaKeywords(),
            //'metaDescription'  => $category->getMetaDescription(),
        );
        echo str_repeat('  ', $depth);
        echo '* '.$category->getName().sprintf(' (%s products)', $category->getProductCount()).PHP_EOL;
        fputcsv($file, $data);
        if ($category->hasChildren()) {
            $children = $this->categoryFactory->create()->getCategories($category->getId());
            foreach ($children as $child) {
                $child = $this->categoryFactory->create()->load($child->getId());
                $this->exportData($child, $file, $depth + 1);
            }
        }
    }
}
