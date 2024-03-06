# frozen_string_literal: true

module ActiveRecord
  module Associations
    class Preloader
      class Batch # :nodoc:
        def initialize(preloaders, available_records:)
          @preloaders = preloaders.reject(&:empty?)
          @available_records = available_records.flatten.group_by { |r| r.class.base_class }
        end

        def call
          branches = @preloaders.flat_map(&:branches)
          # puts "CALLING BATCH PRELOADER from #{caller[0]}"
          # puts branches.inspect
          until branches.empty?
            loaders = branches.flat_map(&:runnable_loaders)

            loaders.each { |loader| loader.associate_records_from_unscoped(@available_records[loader.klass.base_class]) }

            if loaders.any?
              # puts "loaders.any? is true"
              future_tables = branches.flat_map do |branch|
                branch.future_classes - branch.runnable_loaders.map(&:klass)
              end.map(&:table_name).uniq

              # puts "FUTURE TABLES"
              # puts future_tables.inspect
              target_loaders = loaders.reject { |l| future_tables.include?(l.table_name)  }
              target_loaders = loaders if target_loaders.empty?

              # puts "TARGET LOADERS"
              # puts target_loaders.inspect
              group_and_load_similar(target_loaders)
              target_loaders.each(&:run)
            end

            finished, in_progress = branches.partition(&:done?)

            branches = in_progress + finished.flat_map(&:children)
          end
        end

        private
          attr_reader :loaders

          def group_and_load_similar(loaders)
            puts "Patch called"
            grouped_loaders = loaders.grep_v(ThroughAssociation).group_by(&:loader_query)

            grouped_loaders.each_pair do |query, similar_loaders|
              query.load_records_in_batch_async(similar_loaders)
            end

            grouped_loaders.each_pair do |query, similar_loaders|
              # puts query
              query.load_records_in_batch(similar_loaders)
            end
          end
      end
    end
  end
end
