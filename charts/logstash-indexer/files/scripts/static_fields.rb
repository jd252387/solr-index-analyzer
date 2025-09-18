module LogstashScripts
  class StaticFields
    def initialize(params)
      @fields = {}
      (params["fields"] || {}).each do |key, value|
        @fields[key] = value
      end
    end

    def apply(event)
      @fields.each do |field, value|
        next unless event.get(field).nil?
        event.set(field, value)
      end
    end
  end
end

def register(params)
  @handler = LogstashScripts::StaticFields.new(params)
end

def filter(event)
  @handler.apply(event)
  [event]
end
