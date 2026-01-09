// Copyright (C) 2017 Julien Viet
// Licensed under the Apache License, Version 2.0

namespace Vertx.PgClient.Codec;

/// <summary>
/// PostgreSQL protocol constants.
/// </summary>
internal static class PgProtocolConstants
{
    // Authentication types
    public const int AuthenticationTypeOk = 0;
    public const int AuthenticationTypeKerberosV5 = 2;
    public const int AuthenticationTypeCleartextPassword = 3;
    public const int AuthenticationTypeMd5Password = 5;
    public const int AuthenticationTypeScmCredential = 6;
    public const int AuthenticationTypeGss = 7;
    public const int AuthenticationTypeGssContinue = 8;
    public const int AuthenticationTypeSspi = 9;
    public const int AuthenticationTypeSasl = 10;
    public const int AuthenticationTypeSaslContinue = 11;
    public const int AuthenticationTypeSaslFinal = 12;

    // Error or Notice field types
    public const byte ErrorOrNoticeSeverity = (byte)'S';
    public const byte ErrorOrNoticeCode = (byte)'C';
    public const byte ErrorOrNoticeMessage = (byte)'M';
    public const byte ErrorOrNoticeDetail = (byte)'D';
    public const byte ErrorOrNoticeHint = (byte)'H';
    public const byte ErrorOrNoticePosition = (byte)'P';
    public const byte ErrorOrNoticeInternalPosition = (byte)'p';
    public const byte ErrorOrNoticeInternalQuery = (byte)'q';
    public const byte ErrorOrNoticeWhere = (byte)'W';
    public const byte ErrorOrNoticeFile = (byte)'F';
    public const byte ErrorOrNoticeLine = (byte)'L';
    public const byte ErrorOrNoticeRoutine = (byte)'R';
    public const byte ErrorOrNoticeSchema = (byte)'s';
    public const byte ErrorOrNoticeTable = (byte)'t';
    public const byte ErrorOrNoticeColumn = (byte)'c';
    public const byte ErrorOrNoticeDataType = (byte)'d';
    public const byte ErrorOrNoticeConstraint = (byte)'n';

    // Backend message types
    public const byte MessageTypeBackendKeyData = (byte)'K';
    public const byte MessageTypeAuthentication = (byte)'R';
    public const byte MessageTypeErrorResponse = (byte)'E';
    public const byte MessageTypeNoticeResponse = (byte)'N';
    public const byte MessageTypeNotificationResponse = (byte)'A';
    public const byte MessageTypeCommandComplete = (byte)'C';
    public const byte MessageTypeParameterStatus = (byte)'S';
    public const byte MessageTypeReadyForQuery = (byte)'Z';
    public const byte MessageTypeParameterDescription = (byte)'t';
    public const byte MessageTypeRowDescription = (byte)'T';
    public const byte MessageTypeDataRow = (byte)'D';
    public const byte MessageTypePortalSuspended = (byte)'s';
    public const byte MessageTypeNoData = (byte)'n';
    public const byte MessageTypeEmptyQueryResponse = (byte)'I';
    public const byte MessageTypeParseComplete = (byte)'1';
    public const byte MessageTypeBindComplete = (byte)'2';
    public const byte MessageTypeCloseComplete = (byte)'3';
    public const byte MessageTypeFunctionResult = (byte)'V';
    public const byte MessageTypeSslYes = (byte)'S';
    public const byte MessageTypeSslNo = (byte)'N';

    // Frontend message types
    public const byte PasswordMessage = (byte)'p';
    public const byte Query = (byte)'Q';
    public const byte Terminate = (byte)'X';
    public const byte Parse = (byte)'P';
    public const byte Bind = (byte)'B';
    public const byte Describe = (byte)'D';
    public const byte Execute = (byte)'E';
    public const byte Close = (byte)'C';
    public const byte Sync = (byte)'S';
    public const byte CopyData = (byte)'d';
    public const byte CopyDone = (byte)'c';
    public const byte CopyFail = (byte)'f';
    public const byte Flush = (byte)'H';
    
    // Aliases for decoder (backend message types)
    public const byte AuthenticationRequest = MessageTypeAuthentication;
    public const byte BackendKeyData = MessageTypeBackendKeyData;
    public const byte BindComplete = MessageTypeBindComplete;
    public const byte CloseComplete = MessageTypeCloseComplete;
    public const byte CommandComplete = MessageTypeCommandComplete;
    public const byte CopyInResponse = (byte)'G';
    public const byte CopyOutResponse = (byte)'H';
    public const byte DataRow = MessageTypeDataRow;
    public const byte EmptyQueryResponse = MessageTypeEmptyQueryResponse;
    public const byte ErrorResponse = MessageTypeErrorResponse;
    public const byte NoData = MessageTypeNoData;
    public const byte NoticeResponse = MessageTypeNoticeResponse;
    public const byte NotificationResponse = MessageTypeNotificationResponse;
    public const byte ParameterDescription = MessageTypeParameterDescription;
    public const byte ParameterStatus = MessageTypeParameterStatus;
    public const byte ParseComplete = MessageTypeParseComplete;
    public const byte PortalSuspended = MessageTypePortalSuspended;
    public const byte ReadyForQuery = MessageTypeReadyForQuery;
    public const byte RowDescription = MessageTypeRowDescription;
}
