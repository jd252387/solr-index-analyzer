require "aws-sdk-s3"
require "logstash/json"

module LogstashScripts
  class S3Fetch
    def initialize(params)
      @bucket_template = params["bucket"] || ""
      @key_template = params["key_template"] || ""
      @version_field = params["version_field"] || ""
      @target_field = params["target_field"] || ""
      @merge = !!params["merge"]
      @content_format = (params["content_format"] || "json").to_s.downcase
      @json_pointer = parse_pointer(params["json_pointer"] || "")
      @client_options = symbolize_keys(params["client_options"] || {})
      @region = params["region"] || ""
      @assume_role_arn = params["assume_role_arn"] || ""
    end

    def apply(event)
      success = false
      begin
        bucket = event.sprintf(@bucket_template)
        key = event.sprintf(@key_template)
        version_id = version_identifier(event)

        response = build_client.get_object(request_params(bucket, key, version_id))
        body = response.body.read
        data = parse_body(event, body)
        data = navigate(event, data)
        success = assign_result(event, data)
      rescue StandardError
        event.tag("s3_lookup_error")
      end

      event.set("[@metadata][s3_lookup][success]", success)
    end

    private

    def version_identifier(event)
      return nil if @version_field.empty?

      value = event.get(@version_field)
      value.nil? || value.to_s.empty? ? nil : value
    end

    def build_client
      options = @client_options.dup
      options[:region] = @region unless @region.to_s.empty?

      if !@assume_role_arn.to_s.empty?
        require "aws-sdk-sts"
        sts = Aws::STS::Client.new(options)
        credentials = sts.assume_role(role_arn: @assume_role_arn, role_session_name: "logstash-indexer").credentials
        options[:credentials] = Aws::Credentials.new(credentials.access_key_id, credentials.secret_access_key, credentials.session_token)
      end

      Aws::S3::Client.new(options)
    end

    def request_params(bucket, key, version_id)
      params = { bucket: bucket, key: key }
      params[:version_id] = version_id if version_id
      params
    end

    def parse_body(event, body)
      return body unless @content_format == "json"

      LogStash::Json.load(body)
    rescue StandardError
      event.tag("s3_response_parse_failure")
      nil
    end

    def navigate(event, data)
      return data if @json_pointer.empty? || data.nil?

      current = data
      @json_pointer.each do |segment|
        case current
        when Hash
          current = current[segment]
        when Array
          index = segment =~ /^\d+$/ ? segment.to_i : nil
          current = index ? current[index] : nil
        else
          current = nil
        end
        break if current.nil?
      end
      current
    rescue StandardError
      event.tag("s3_pointer_failure")
      nil
    end

    def assign_result(event, data)
      return false if data.nil?

      if data.is_a?(Hash)
        if @merge
          existing = event.get(@target_field)
          if existing.is_a?(Hash)
            event.set(@target_field, existing.merge(data))
          else
            event.set(@target_field, data)
          end
        else
          event.set(@target_field, data)
        end
      else
        event.set(@target_field, data)
      end
      true
    end

    def symbolize_keys(source)
      source.each_with_object({}) do |(key, value), memo|
        memo[key.to_sym] = value
      end
    end

    def parse_pointer(pointer)
      stripped = pointer.to_s.sub(%r{^/+}, "")
      return [] if stripped.empty?

      stripped.split("/").map { |segment| segment.gsub("~1", "/").gsub("~0", "~") }
    end
  end
end

def register(params)
  @handler = LogstashScripts::S3Fetch.new(params)
end

def filter(event)
  @handler.apply(event)
  [event]
end
