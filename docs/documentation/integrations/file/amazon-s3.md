# Amazon S3

## Credentials

Hypha's Amazon S3 integration uses the AWS SDK's credential chain to authenticate requests. This means you don't need to explicitly provide access keys in your application code or environment variables. Instead, the SDK will automatically look for credentials in the following order:

1. Environment variables
2. Shared credential file (\~/.aws/credentials)
3. AWS IAM role for Amazon EC2 or ECS tasks

### Setting Up Credentials

To set up your credentials, you have several options:

1. **AWS CLI Configuration**: If you have the AWS CLI installed, you can run `aws configure` to set up your credentials. This will create a shared credential file.
2. **Shared Credentials File**: Manually create or edit the file `~/.aws/credentials` (on Linux/Mac) or `%UserProfile%\.aws\credentials` (on Windows) with the following content:

```conf
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

3. **Environment Variables**: Set the following environment variables:

```bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

4. **IAM Roles**: If your application is running on an AWS EC2 instance or ECS task, you can assign an IAM role with the necessary permissions to access S3.

### Region and Bucket Configuration

Region and bucket name are specified in your Hypha source configuration.

### Hypha Source and Model Configuration for Amazon S3

```yaml
# FILENAME: ~/.hypha/models/users.yaml
name: users
type: file
file_patterns:
  - "users/v1/**.csv" # This will match all csv files under the users/v1 prefix
format: csv
options:
  auto_detect: true
  header: true
  delim: ","
  quote: "\""
  escape: "\""
```

```yaml
# FILENAME: ~/.hypha/sources.yaml
sources:
  - name: users-s3-us-east-1
    engine: s3
    connection:
        bucket_name: users
        region: us-east-1
    models:
      - users
```
