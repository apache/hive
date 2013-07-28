package org.apache.hadoop.hive.thrift;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;

public class DBTokenStore implements DelegationTokenStore {


  @Override
  public int addMasterKey(String s) throws TokenStoreException {
    return (Integer)invokeOnRawStore("addMasterKey", new Object[]{s},String.class);
  }

  @Override
  public void updateMasterKey(int keySeq, String s) throws TokenStoreException {
    invokeOnRawStore("updateMasterKey", new Object[] {Integer.valueOf(keySeq), s},
        Integer.class, String.class);
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
    return (Boolean)invokeOnRawStore("removeMasterKey", new Object[] {Integer.valueOf(keySeq)},
      Integer.class);
  }

  @Override
  public String[] getMasterKeys() throws TokenStoreException {
    return (String[])invokeOnRawStore("getMasterKeys", null, null);
  }

  @Override
  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) throws TokenStoreException {

    try {
      String identifier = TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier);
      String tokenStr = Base64.encodeBase64URLSafeString(
        HiveDelegationTokenSupport.encodeDelegationTokenInformation(token));
      return (Boolean)invokeOnRawStore("addToken", new Object[] {identifier, tokenStr},
        String.class, String.class);
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier)
      throws TokenStoreException {
    try {
      String tokenStr = (String)invokeOnRawStore("getToken", new Object[] {
          TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
      return (null == tokenStr) ? null : HiveDelegationTokenSupport.decodeDelegationTokenInformation(Base64.decodeBase64(tokenStr));
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) throws TokenStoreException{
    try {
      return (Boolean)invokeOnRawStore("removeToken", new Object[] {
        TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
    } catch (IOException e) {
      throw new TokenStoreException(e);
    }
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() throws TokenStoreException{

    List<String> tokenIdents = (List<String>)invokeOnRawStore("getAllTokenIdentifiers", null, null);
    List<DelegationTokenIdentifier> delTokenIdents = new ArrayList<DelegationTokenIdentifier>(tokenIdents.size());

    for (String tokenIdent : tokenIdents) {
      DelegationTokenIdentifier delToken = new DelegationTokenIdentifier();
      try {
        TokenStoreDelegationTokenSecretManager.decodeWritable(delToken, tokenIdent);
      } catch (IOException e) {
        throw new TokenStoreException(e);
      }
      delTokenIdents.add(delToken);
    }
    return delTokenIdents;
  }

  private Object hmsHandler;

  @Override
  public void setStore(Object hms) throws TokenStoreException {
    hmsHandler = hms;
  }

  private Object invokeOnRawStore(String methName, Object[] params, Class<?> ... paramTypes)
      throws TokenStoreException{

    try {
      Object rawStore = hmsHandler.getClass().getMethod("getMS").invoke(hmsHandler);
      return rawStore.getClass().getMethod(methName, paramTypes).invoke(rawStore, params);
    } catch (IllegalArgumentException e) {
        throw new TokenStoreException(e);
    } catch (SecurityException e) {
        throw new TokenStoreException(e);
    } catch (IllegalAccessException e) {
        throw new TokenStoreException(e);
    } catch (InvocationTargetException e) {
        throw new TokenStoreException(e.getCause());
    } catch (NoSuchMethodException e) {
        throw new TokenStoreException(e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    // No-op
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void close() throws IOException {
    // No-op.
  }


}
