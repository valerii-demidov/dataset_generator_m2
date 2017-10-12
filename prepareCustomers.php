<?php

/**
 * Class prepareCustomers
 */
class prepareCustomers
{
    const SAMPLE_FILE_PATH = 'importExport/Customer/sample/FakeNameGenerator.csv';
    const SAMPLE_CUSTOMER_LIVE = 'importExport/Customer/live/customer_live.csv';
    const SAMPLE_CUSTOMER_ADDRESS_LIVE = 'importExport/Customer/live/customer_address.csv';

    protected $sampleCustomerFile;
    protected $sampleCustomerAddressFile;


    protected $customerHeader = [
        'email',
        '_website',
        '_store',
        'confirmation',
        'created_at',
        'created_in',
        'disable_auto_group_change',
        'dob',
        'failures_num',
        'firstname',
        'first_failure',
        'gender',
        'group_id',
        'lastname',
        'lock_expires',
        'middlename',
        'password_hash',
        'prefix',
        'rp_token',
        'rp_token_created_at',
        'store_id',
        'suffix',
        'taxvat',
        'updated_at',
        'website_id',
        'password',
    ];

    protected $customerAddressHeader = [
        '_website',
        '_email',
        //'_entity_id',
        'city',
        'company',
        'country_id',
        'fax',
        'firstname',
        'lastname',
        'middlename',
        'postcode',
        'prefix',
        'region',
        'region_id',
        'street',
        'suffix',
        'telephone',
        'vat_id',
        'vat_is_valid',
        'vat_request_date',
        'vat_request_id',
        'vat_request_success',
        '_address_default_billing_',
        '_address_default_shipping_',
    ];

    protected $customerDataAssoc = [

    ];

    public function readSampleCustomer()
    {
        $row = 1;
        $fileCustomer = fopen(static::SAMPLE_CUSTOMER_LIVE, 'w');
        $fileCustomerAddress = fopen(static::SAMPLE_CUSTOMER_ADDRESS_LIVE, 'w');
        $directoryCountryRegion = $this->connectMysqlDb();

        if (($sampleFile = fopen(static::SAMPLE_FILE_PATH, "r")) !== false) {
            while (($data = fgetcsv($sampleFile, 1000, ",")) !== false) {
                if ($row == 1) {
                    /*$this->writeLiveRow($fileCustomer, $this->customerHeader);
                    $this->writeLiveRow($fileCustomerAddress, $this->customerAddressHeader);*/

                    $mergedHeader = [
                        'email',
                        '_website',
                        '_store',
                        'confirmation',
                        'created_at',
                        'created_in',
                        'disable_auto_group_change',
                        'dob',
                        'failures_num',
                        'firstname',
                        'first_failure',
                        'gender',
                        'group_id',
                        'lastname',
                        'lock_expires',
                        'middlename',
                        'password_hash',
                        'prefix',
                        'rp_token',
                        'rp_token_created_at',
                        'store_id',
                        'suffix',
                        'taxvat',
                        'updated_at',
                        'website_id',
                        'password',

                        '_address_city',
                        '_address_country_id',
                        '_address_postcode',
                        '_address_region',
                        '_address_street',
                        '_address_telephone',
                        '_address_firstname',
                        '_address_lastname',
                        '_address_middlename',
                        '_address_default_billing_',
                        '_address_default_shipping_',
                    ];
                    $this->writeLiveRow($fileCustomer, $mergedHeader);

                    $row++;
                    continue;
                }

                $dob = strtotime($data[21]); //'1973-12-15';
                $dob = date('Y-m-d', $dob);
                $createdAt = date('Y-m-d h:m:i'); //2017-09-27 08:02:41';
                $gender = ($data[3] == 'Mrs.') ? 'Male' : 'Female';

                /*$customerData = [
                    'email' => $data[14],
                    '_website' => 'base',
                    '_store' => 'default',
                    'confirmation' => '',
                    'created_at' => $createdAt,
                    'created_in' => 'Default Store View',
                    'disable_auto_group_change' => '',
                    'dob' => $dob,
                    'failures_num' => 0,
                    'firstname' => $data[4],
                    'first_failure' => '',
                    'gender' => ucfirst($gender),
                    'group_id' => 1,
                    'lastname' => $data[6],
                    'lock_expires' => '',
                    'middlename' => $data[5],
                    'password_hash' => '',
                    'prefix' => '',
                    'rp_token' => '',
                    'rp_token_created_at' => '',
                    'store_id' => 1,
                    'suffix' => '',
                    'taxvat' => '',
                    'updated_at' => '',
                    'website_id' => 1,
                    'password' => 'admin123',
                ];


                $customerAddressData = [
                    '_website' => 'base',
                    '_email',
                    //'_entity_id',
                    'city' => $data[8],
                    'company' => '',
                    'country_id' => 'US',
                    'fax' => '',
                    'firstname' => $data[4],
                    'lastname' => $data[6],
                    'middlename' => $data[5],
                    'postcode' => $data[5],
                    'prefix' => '',
                    'region' => $data[10],
                    'region_id' => ($data[10] && isset($directoryCountryRegion[$data[10]])) ? $directoryCountryRegion[$data[10]] : '',
                    'street' => $data[11],
                    'suffix' => '',
                    'telephone' => $data[18],
                    'vat_id' => '',
                    'vat_is_valid' => '',
                    'vat_request_date' => '',
                    'vat_request_id' => '',
                    'vat_request_success' => '',
                    '_address_default_billing_' => 1,
                    '_address_default_shipping_' => 1,
                ];

                $this->writeLiveRow($fileCustomer, $customerData);
                $this->writeLiveRow($fileCustomerAddress, $customerAddressData);*/


                $customerData = [
                    'email' => $data[14],
                    '_website' => 'base',
                    '_store' => 'default',
                    'confirmation' => '',
                    'created_at' => $createdAt,
                    'created_in' => 'Default Store View',
                    'disable_auto_group_change' => '',
                    'dob' => $dob,
                    'failures_num' => 0,
                    'firstname' => $data[4],
                    'first_failure' => '',
                    'gender' => $gender,   ///////////
                    'group_id' => 1,
                    'lastname' => $data[6],
                    'lock_expires' => '',
                    'middlename' => $data[5],
                    'password_hash' => '',
                    'prefix' => '',
                    'rp_token' => '',
                    'rp_token_created_at' => '',
                    'store_id' => 1,
                    'suffix' => '',
                    'taxvat' => '',
                    'updated_at' => '',
                    'website_id' => 1,
                    'password' => 'admin123',

                    '_address_city' => $data[8],
                    '_address_country_id' => 'US',
                    '_address_postcode' => $data[11],
                    '_address_region' => $data[10],
                    //'region_id' => ($data[10] && isset($directoryCountryRegion[$data[10]])) ? $directoryCountryRegion[$data[10]] : '',
                    '_address_street' => $data[7],
                    '_address_telephone' => $data[18],
                    '_address_firstname' => $data[4],
                    '_address_lastname' => $data[6],
                    '_address_middlename' => $data[5],
                    '_address_default_billing_' => 1,
                    '_address_default_shipping_' => 1,
                ];

                $this->writeLiveRow($fileCustomer, $customerData);

                if ($row == 1001) {
                    die;
                }
                $row++;
            }
            //fclose($sampleFile);
            //fclose($fileCustomer);
        }
    }

    /**
     * @param $fileHandler
     * @param array $row
     */
    public function writeLiveRow($fileHandler, array $row)
    {
        fputcsv($fileHandler, $row);
    }

    public function connectMysqlDb()
    {
        $mysqli = new mysqli("localhost", "root", "123", "pp_mc219");
        $result = $mysqli->query("SELECT * FROM `directory_country_region`");
        $row = $result->fetch_all();

        $regionData = [];
        foreach ($row as $key => $value) {
            $regionData[$value[3]] = $value[0];
        }

        return $regionData;
    }
}

$prepareCustomers = new prepareCustomers();
$prepareCustomers->readSampleCustomer();

