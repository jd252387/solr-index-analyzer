require "logstash/json"

module LogstashScripts
  class InlinePayload
    def initialize(params)
      @payload_field = params["payload_field"] || ""
      @target_field = params["target_field"] || ""
      @payload_is_json = !!params["payload_is_json"]
      @fallback_enabled = !!params["fallback_enabled"]
      sources = Array(params["fallback_sources"])
      @fallback_sources = sources.map do |entry|
        {
          "name" => entry["name"],
          "path" => entry["path"]
        }
      end
    end

    def apply(event)
      data = read_payload(event)
      return if data.nil?

      if data.is_a?(Hash)
        event.set(@target_field, data)
      else
        event.set(@target_field, data)
      end
    end

    private

    def read_payload(event)
      value = nil
      value = event.get(@payload_field) unless @payload_field.empty?
      value = parse_json(event, value) if @payload_is_json
      return value unless value.nil?
      return nil unless @fallback_enabled

      collected = collect_from_root(event)
      collected
    end

    def parse_json(event, value)
      return value unless value.is_a?(String)

      LogStash::Json.load(value)
    rescue StandardError
      event.tag("inline_payload_parse_failure")
      nil
    end

    def collect_from_root(event)
      data = {}
      @fallback_sources.each do |source|
        name = source["name"]
        path = source["path"]
        next if path.nil? || path.empty?

        value = event.get(path)
        next if value.nil?

        data[name] = value
      end
      data.empty? ? nil : data
    end
  end
end

def register(params)
  @handler = LogstashScripts::InlinePayload.new(params)
end

def filter(event)
  @handler.apply(event)
  [event]
end
