class Hiera
  module Backend
    class Multiyaml_backend
      # XXX: There is no option to override the datadir/datafile function logic
      # in Hiera::Backend, so there is a lot of copy paste code from
      # Hiera::Backend and Hiera::Backend::Yaml_backend instead of inheritance
      # or just calling it from here. The Hiera architecture sucks big dongs. :-(

      def initialize(cache=nil)
        require 'yaml'
        Hiera.debug("Hiera MultiYAML-derp backend starting")

        @cache = cache || Filecache.new
      end

      def lookup(key, scope, order_override, resolution_type)
        Hiera.debug("MultiYAML: Entering lookup method in MultiYAML backend!")
        answer = nil

        Config[:multiyaml][:backends].each do |backend|
          backend = backend.to_sym

          Hiera.debug("MultiYAML: Starting with backend #{backend}")
          #Backend.datasourcefiles(:yaml, scope, "yaml", order_override) do |source, yamlfile|
          Backend.datasources(scope, order_override) do |source|
            Hiera.debug("MultiYAML: Looking for data source #{source} in MultiYAML #{backend}")
            yamlfile = Backend.parse_answer(File.join(Config[backend][:datadir], "#{source}.yaml"), scope)
            Hiera.debug("MultiYAML: Overriding yamlfile variable with #{yamlfile}")

            if not File.exist?(yamlfile)
              Hiera.debug("MultiYAML: Cannot find datafile #{yamlfile}, skipping")
              next
            end

            Hiera.debug("MultiYAML: Found datafile #{yamlfile}, hooray!")
            data = @cache.read_file(yamlfile, Hash) do |data|
              YAML.load(data) || {}
            end

            next if data.empty?
            next unless data.include?(key)

            Hiera.debug("MultiYAML: Found #{key} in #{backend}/#{source} with resolution_type #{resolution_type}")

            new_answer = Backend.parse_answer(data[key], scope)
            answer = merge_answer(resolution_type, new_answer, answer)
            if resolution_type == :priority
              break
            end
          end
          Hiera.debug("MultiYAML: Done with backend #{backend}")
        end

        Hiera.debug("MultiYAML: Leaving lookup method in MultiYAML backend!  Answer is #{answer}")
        return answer
      end

      def merge_answer(resolution_type, new_answer, answer)
        if not new_answer.nil?
          case resolution_type.is_a?(Hash) ? :hash : resolution_type
          when :array
            raise Exception, "Hiera type mismatch: expected Array and got #{new_answer.class}" unless new_answer.kind_of? Array or new_answer.kind_of? String
            answer ||= []
            answer << new_answer
          when :hash
            raise Exception, "Hiera type mismatch: expected Hash and got #{new_answer.class}" unless new_answer.kind_of? Hash
            answer ||= {}
            answer = Hiera::Backend.merge_answer(new_answer,answer)
          else
            answer = new_answer
          end
        end
        return answer
      end
    end
  end
end
