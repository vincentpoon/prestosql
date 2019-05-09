/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.argus;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;
import static io.prestosql.spi.ErrorType.INTERNAL_ERROR;

public enum ArgusErrorCode
        implements ErrorCodeSupplier
{
    ARGUS_INTERNAL_ERROR(0, INTERNAL_ERROR),
    ARGUS_IO_ERROR(1, EXTERNAL),
    AUTH_ERROR(2, EXTERNAL),
    ARGUS_QUERY_ERROR(3, EXTERNAL);

    private final ErrorCode errorCode;

    ArgusErrorCode(int code, ErrorType type)
    {
        // salesforce specific error code, should be unique:
        // https://github.com/prestosql/presto/wiki/Error-Codes
        errorCode = new ErrorCode(code + 0x0903_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
