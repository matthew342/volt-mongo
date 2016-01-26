require 'mongo'
require 'volt/utils/data_transformer'

# We need to be able to deeply stringify keys for mongo
class Hash
  def nested_stringify_keys
    self.stringify_keys.map do |key, value|
      if value.is_a?(Hash)
        value = value.nested_stringify_keys
      end

      [key, value]
    end.to_h
  end
end

module Volt
  class DataStore
    class MongoAdaptorServer < BaseAdaptorServer
      attr_reader :db, :mongo_db

      # check if the database can be connected to.
      # @return Boolean
      def connected?
        begin
          db

          true
        rescue ::Mongo::Error => e
          false
        end
      end

      def db
        return @db if @db

        # change deprecated keys to match new format.
        Volt.config.to_h.keys.grep(/^DB_/i).each do |db_key|
          key = db_key.to_s.sub(/db_/i, '')
          Volt.logger.warn "config.db_* keys are deprecated. Please change config.#{db_key} to config.db.#{key} in your config/app.rb."
          Volt.configure do |c|
            c.send("#{key.downcase}=", Volt.config.to_h[db_key])
          end
        end

        # set default host and port if not set.
        Volt.configure do |c|
          c.db.host = '127.0.0.1' unless Volt.config.db.host
          c.db.port = '27017' unless Volt.config.db.port
        end

        db_name = Volt.config.db.uri.try(:split, '/').try(:last) || Volt.config.db.database || Volt.config.db.name
        if Volt.config.db.uri.present?
          @db ||= ::Mongo::Client.new(Volt.config.db.uri, database: db_name, :monitoring => false)
        else
          @db ||= ::Mongo::Client.new("mongodb://#{Volt.config.db.host}:#{Volt.config.db.port}", database: db_name, :monitoring => false)
        end

        @db
      end

      def insert(collection, values)
        db[collection].insert_one(values)
      end

      def update(collection, values)
        values = values.nested_stringify_keys
        values = Volt::DataTransformer.transform(values) do |value|
          if defined?(VoltTime) && value.is_a?(VoltTime)
            value.to_time
          else
            value
          end
        end

        to_mongo_id!(values)
        # TODO: Seems mongo is dumb and doesn't let you upsert with custom id's
        begin
          db[collection].insert_one(values)
        rescue => error
          # Really mongo client?
          msg = error.message
          if (msg[/^E11000/] || msg[/^insertDocument :: caused by :: 11000 E11000/]) && msg['$_id_']
            # Update because the id already exists
            update_values = values.dup
            id = update_values.delete('_id')
            db[collection].update_one({ '_id' => id }, update_values)
          else
            return { error: error.message }
          end
        end

        nil
      end

      def query(collection, query)
        if ENV['DB_LOG'] && collection.to_s != 'active_volt_instances'
          Volt.logger.info("Query: #{collection}: #{query.inspect}")
        end

        allowed_methods = %w(find skip limit sort)

        result = db[collection]

        query.each do |query_part|
          method_name, *args = query_part

          unless allowed_methods.include?(method_name.to_s)
            fail "`#{method_name}` is not part of a valid query"
          end

          args = args.map do |arg|
            if arg.is_a?(Hash)
              arg = arg.stringify_keys
            end
            arg
          end

          if method_name == 'find' && args.size > 0
            qry = args[0]
            to_mongo_id!(qry)
          end

          result = result.send(method_name, *args)
        end

        if result.is_a?(::Mongo::Collection::View)
          result = result.to_a.map do |hash|
            # Return id instead of _id
            to_volt_id!(hash)

            # Volt expects symbol keys
            hash.symbolize_keys
          end#.tap {|v| puts "QUERY: " + v.inspect }
        end

        values = Volt::DataTransformer.transform(result) do |value|
          if defined?(VoltTime) && value.is_a?(Time)
            value = VoltTime.from_time(value)
          else
            value
          end
        end

        values
      end

      def delete(collection, query)
        if query.key?('id')
          query['_id'] = query.delete('id')
        end

        db[collection].delete_one(query)
      end

      # remove the collection entirely
      def drop_collection(collection)
        db[collection].drop
      end

      def drop_database
        db.database.drop
      end

      def adapter_version
        ::Mongo::VERSION
      end

      private
      # Mutate a hash to use id instead of _id
      def to_volt_id!(hash)
        if hash.key?('_id')
          # Run to_s to convert BSON::Id also
          hash['id'] = hash.delete('_id').to_s
        end
      end

      # Mutate a hash to use _id instead of id
      def to_mongo_id!(hash)
        if hash.key?('id')
          hash['_id'] = hash.delete('id')
        end
      end

    end
  end
end
