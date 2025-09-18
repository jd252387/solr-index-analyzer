module LogstashScripts
  class MongoMerge
    def initialize(params)
      @result_field = params["result_field"] || ""
      @target_field = params["target_field"] || ""
      @merge = !!params["merge"]
      @pointer = parse_pointer(params["pointer"] || "")
    end

    def apply(event)
      success = false
      data = event.get(@result_field)
      data = data.first if data.is_a?(Array)
      data = navigate(event, data)

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
        success = true
      elsif !data.nil?
        event.set(@target_field, data)
        success = true
      end

      event.set("[@metadata][mongo_lookup][success]", success)
    end

    private

    def parse_pointer(pointer)
      stripped = pointer.to_s.sub(%r{^/+}, "")
      return [] if stripped.empty?

      stripped.split("/").map { |segment| decode_pointer_segment(segment) }
    end

    def decode_pointer_segment(segment)
      segment.gsub("~1", "/").gsub("~0", "~")
    end

    def navigate(event, data)
      return data if @pointer.empty? || data.nil?

      current = data
      @pointer.each do |segment|
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
      event.tag("mongo_result_parse_failure")
      nil
    end
  end
end

def register(params)
  @handler = LogstashScripts::MongoMerge.new(params)
end

def filter(event)
  @handler.apply(event)
  [event]
end
