package com.microsoft.kusto.spark.utils

import java.io.IOException

import com.microsoft.azure.CloudException
import com.microsoft.azure.keyvault.KeyVaultClient
import com.microsoft.kusto.spark.datasource._

object KeyVaultUtils {
  val AppId = "kustoAppId"
  val AppKey = "kustoAppKey"
  val AppAuthority = "kustoAppAuthority"
  val SasUrl = "blobStorageSasUrl"
  val StorageAccountId = "blobStorageAccountName"
  val StorageAccountKey = "blobStorageAccountKey"
  val Container = "blobContainer"
  var cachedClient: KeyVaultClient = _

  private def getClient(clientID: String, clientPassword: String): KeyVaultClient ={
    if(cachedClient == null) {
      cachedClient = new KeyVaultADALAuthenticator(clientID, clientPassword).getAuthenticatedClient
    }
    cachedClient
  }

  @throws[CloudException]
  @throws[IOException]
  def getStorageParamsFromKeyVault(keyVaultAuthentication: KeyVaultAuthentication): KustoStorageParameters = {
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.keyVaultAppID, app.keyVaultAppKey)
        getStorageParamsFromKeyVaultImpl(client, app.uri)
      case certificate: KeyVaultCertificateAuthentication => throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  @throws[CloudException]
  @throws[IOException]
  def getAadAppParametersFromKeyVault(keyVaultAuthentication: KeyVaultAuthentication): AadApplicationAuthentication={
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.keyVaultAppID, app.keyVaultAppKey)
        getAadAppParamsFromKeyVaultImpl(client, app.uri)
      case certificate: KeyVaultCertificateAuthentication => throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  private def getAadAppParamsFromKeyVaultImpl(client: KeyVaultClient, uri: String): AadApplicationAuthentication ={
    val id = client.getSecret(uri, AppId)
    val key = client.getSecret(uri, AppKey)

    var authority = client.getSecret(uri, AppAuthority).value()
    if(authority.isEmpty){
      authority = "microsoft.com"
    }

    AadApplicationAuthentication(
      ID = if (id == null) null else id.value(),
      password = if (key == null) null else key.value(),
      authority = authority)
  }

  private def getStorageParamsFromKeyVaultImpl(client: KeyVaultClient, uri: String): KustoStorageParameters = {
    val sasUrl = Option(client.getSecret(uri, SasUrl))

    val accountId =  Option(client.getSecret(uri, StorageAccountId))
    val accountKey = Option(client.getSecret(uri, StorageAccountKey))
    val container = Option(client.getSecret(uri, Container))

    if(sasUrl.isEmpty) {
      KustoStorageParameters(
        account = if(accountId.isDefined) accountId.get.value else "",
        secret = if (accountKey.isDefined) accountKey.get.value else "",
        container = if (container.isDefined) container.get.value else "",
        secretIsAccountKey = true)
    } else {
      KustoDataSourceUtils.parseSas(sasUrl.get.value)
    }
  }
}
