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

class ObjectDictionary
{
    static public $isValidate = false;

    static public $_TSPEC = array(
        1 => array(
            'var' => 'values',
            'isRequired' => true,
            'type' => TType::MAP,
            'ktype' => TType::STRING,
            'vtype' => TType::LST,
            'key' => array(
                'type' => TType::STRING,
            ),
            'val' => array(
                'type' => TType::LST,
                'etype' => TType::STRING,
                'elem' => array(
                    'type' => TType::STRING,
                    ),
                ),
        ),
    );

    /**
     * @var array
     */
    public $values = null;

    public function __construct($vals = null)
    {
        if (is_array($vals)) {
            if (isset($vals['values'])) {
                $this->values = $vals['values'];
            }
        }
    }

    public function getName()
    {
        return 'ObjectDictionary';
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
                        $this->values = array();
                        $_size296 = 0;
                        $_ktype297 = 0;
                        $_vtype298 = 0;
                        $xfer += $input->readMapBegin($_ktype297, $_vtype298, $_size296);
                        for ($_i300 = 0; $_i300 < $_size296; ++$_i300) {
                            $key301 = '';
                            $val302 = array();
                            $xfer += $input->readString($key301);
                            $val302 = array();
                            $_size303 = 0;
                            $_etype306 = 0;
                            $xfer += $input->readListBegin($_etype306, $_size303);
                            for ($_i307 = 0; $_i307 < $_size303; ++$_i307) {
                                $elem308 = null;
                                $xfer += $input->readString($elem308);
                                $val302 []= $elem308;
                            }
                            $xfer += $input->readListEnd();
                            $this->values[$key301] = $val302;
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
        $xfer += $output->writeStructBegin('ObjectDictionary');
        if ($this->values !== null) {
            if (!is_array($this->values)) {
                throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
            }
            $xfer += $output->writeFieldBegin('values', TType::MAP, 1);
            $output->writeMapBegin(TType::STRING, TType::LST, count($this->values));
            foreach ($this->values as $kiter309 => $viter310) {
                $xfer += $output->writeString($kiter309);
                $output->writeListBegin(TType::STRING, count($viter310));
                foreach ($viter310 as $iter311) {
                    $xfer += $output->writeString($iter311);
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
