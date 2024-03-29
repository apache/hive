<?php
namespace metastore;

/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;
use Thrift\Type\TMessageType;
use Thrift\Exception\TException;
use Thrift\Exception\TProtocolException;
use Thrift\Protocol\TProtocol;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Exception\TApplicationException;

class DoubleColumnStatsData
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'lowValue',
            'isRequired' => false,
            'type' => TType::DOUBLE,
        ),
        2 => array(
            'var' => 'highValue',
            'isRequired' => false,
            'type' => TType::DOUBLE,
        ),
        3 => array(
            'var' => 'numNulls',
            'isRequired' => true,
            'type' => TType::I64,
        ),
        4 => array(
            'var' => 'numDVs',
            'isRequired' => true,
            'type' => TType::I64,
        ),
        5 => array(
            'var' => 'bitVectors',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
        6 => array(
            'var' => 'histogram',
            'isRequired' => false,
            'type' => TType::STRING,
        ),
    );

    /**
     * @var double
     */
    public $lowValue = null;
    /**
     * @var double
     */
    public $highValue = null;
    /**
     * @var int
     */
    public $numNulls = null;
    /**
     * @var int
     */
    public $numDVs = null;
    /**
     * @var string
     */
    public $bitVectors = null;
    /**
     * @var string
     */
    public $histogram = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['lowValue'])) {
                $this->lowValue = $vals['lowValue'];
            }
            if (isset($vals['highValue'])) {
                $this->highValue = $vals['highValue'];
            }
            if (isset($vals['numNulls'])) {
                $this->numNulls = $vals['numNulls'];
            }
            if (isset($vals['numDVs'])) {
                $this->numDVs = $vals['numDVs'];
            }
            if (isset($vals['bitVectors'])) {
                $this->bitVectors = $vals['bitVectors'];
            }
            if (isset($vals['histogram'])) {
                $this->histogram = $vals['histogram'];
            }
        }
    }

    public function getName()
    {
        return 'DoubleColumnStatsData';
    }


    public function read($input)
    {
        $xfer = 0;
        $fname = null;
        $ftype = 0;
        $fid = 0;
        $xfer += $input->readStructBegin($fname);
        while (true) {
            $xfer += $input->readFieldBegin($fname, $ftype, $fid);
            if ($ftype == TType::STOP) {
                break;
            }
            switch ($fid) {
                case 1:
                    if ($ftype == TType::DOUBLE) {
                        $xfer += $input->readDouble($this->lowValue);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 2:
                    if ($ftype == TType::DOUBLE) {
                        $xfer += $input->readDouble($this->highValue);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 3:
                    if ($ftype == TType::I64) {
                        $xfer += $input->readI64($this->numNulls);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 4:
                    if ($ftype == TType::I64) {
                        $xfer += $input->readI64($this->numDVs);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 5:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->bitVectors);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                case 6:
                    if ($ftype == TType::STRING) {
                        $xfer += $input->readString($this->histogram);
                    } else {
                        $xfer += $input->skip($ftype);
                    }
                    break;
                default:
                    $xfer += $input->skip($ftype);
                    break;
            }
            $xfer += $input->readFieldEnd();
        }
        $xfer += $input->readStructEnd();
        return $xfer;
    }

    public function write($output)
    {
        $xfer = 0;
        $xfer += $output->writeStructBegin('DoubleColumnStatsData');
        if ($this->lowValue !== null) {
            $xfer += $output->writeFieldBegin('lowValue', TType::DOUBLE, 1);
            $xfer += $output->writeDouble($this->lowValue);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->highValue !== null) {
            $xfer += $output->writeFieldBegin('highValue', TType::DOUBLE, 2);
            $xfer += $output->writeDouble($this->highValue);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->numNulls !== null) {
            $xfer += $output->writeFieldBegin('numNulls', TType::I64, 3);
            $xfer += $output->writeI64($this->numNulls);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->numDVs !== null) {
            $xfer += $output->writeFieldBegin('numDVs', TType::I64, 4);
            $xfer += $output->writeI64($this->numDVs);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->bitVectors !== null) {
            $xfer += $output->writeFieldBegin('bitVectors', TType::STRING, 5);
            $xfer += $output->writeString($this->bitVectors);
            $xfer += $output->writeFieldEnd();
        }
        if ($this->histogram !== null) {
            $xfer += $output->writeFieldBegin('histogram', TType::STRING, 6);
            $xfer += $output->writeString($this->histogram);
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}
