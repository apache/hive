<?php
namespace metastore;

/**
 * Autogenerated by Thrift Compiler (0.13.0)
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

class PartitionsStatsResult
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'partStats',
            'isRequired' => true,
            'type' => TType::MAP,
            'ktype' => TType::STRING,
            'vtype' => TType::LST,
            'key' => array(
                'type' => TType::STRING,
            ),
            'val' => array(
                'type' => TType::LST,
                'etype' => TType::STRUCT,
                'elem' => array(
                    'type' => TType::STRUCT,
                    'class' => '\metastore\ColumnStatisticsObj',
                    ),
                ),
        ),
    );

    /**
     * @var array
     */
    public $partStats = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['partStats'])) {
                $this->partStats = $vals['partStats'];
            }
        }
    }

    public function getName()
    {
        return 'PartitionsStatsResult';
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
                    if ($ftype == TType::MAP) {
                        $this->partStats = array();
                        $_size389 = 0;
                        $_ktype390 = 0;
                        $_vtype391 = 0;
                        $xfer += $input->readMapBegin($_ktype390, $_vtype391, $_size389);
                        for ($_i393 = 0; $_i393 < $_size389; ++$_i393) {
                            $key394 = '';
                            $val395 = array();
                            $xfer += $input->readString($key394);
                            $val395 = array();
                            $_size396 = 0;
                            $_etype399 = 0;
                            $xfer += $input->readListBegin($_etype399, $_size396);
                            for ($_i400 = 0; $_i400 < $_size396; ++$_i400) {
                                $elem401 = null;
                                $elem401 = new \metastore\ColumnStatisticsObj();
                                $xfer += $elem401->read($input);
                                $val395 []= $elem401;
                            }
                            $xfer += $input->readListEnd();
                            $this->partStats[$key394] = $val395;
                        }
                        $xfer += $input->readMapEnd();
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
        $xfer += $output->writeStructBegin('PartitionsStatsResult');
        if ($this->partStats !== null) {
            if (!is_array($this->partStats)) {
                throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
            }
            $xfer += $output->writeFieldBegin('partStats', TType::MAP, 1);
            $output->writeMapBegin(TType::STRING, TType::LST, count($this->partStats));
            foreach ($this->partStats as $kiter402 => $viter403) {
                $xfer += $output->writeString($kiter402);
                $output->writeListBegin(TType::STRUCT, count($viter403));
                foreach ($viter403 as $iter404) {
                    $xfer += $iter404->write($output);
                }
                $output->writeListEnd();
            }
            $output->writeMapEnd();
            $xfer += $output->writeFieldEnd();
        }
        $xfer += $output->writeFieldStop();
        $xfer += $output->writeStructEnd();
        return $xfer;
    }
}