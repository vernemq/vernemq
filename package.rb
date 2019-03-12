#!/usr/bin/env ruby
$: << File.join(File.dirname(__FILE__), "..", "..", "lib")


#######
# TODO:
#
# 1. Auto create vernemq user
#
# Missing:
# 1. vernemq attach (because it's started as vernemq foreground)
# 2.

# This example uses the API to create a package from local files
# it also creates necessary init-scripts and systemd files so our executable can be used as a service

require "fpm"
require "tmpdir"
require "fpm/package/pleaserun"

# enable logging
FPM::Util.send :module_function, :logger
FPM::Util.logger.level = :info
FPM::Util.logger.subscribe STDERR

package = FPM::Package::Dir.new

# Set some attributes
package.name = "vernemq"
package.version = "1.7.1"

# Add a script to run after install (should be in the current directory):
# package.scripts[:after_install] = "files/create_user.postinstsss"

# Example for adding special attributes
package.attributes[:deb_group] = "vernemq"
package.attributes[:deb_user] = "vernemq"
package.attributes[:rpm_group] = "vernemq"
package.attributes[:rpm_user] = "vernemq"
#package.attributes[:pleaserun_user] = "vernemq"
#package.attributes[:pleaserun_group] = "vernemq"

# Add our files (should be in the current directory):
package.input("files/vernemq-wrapper=/usr/bin/vernemq")
package.input("files/vmq-admin-wrapper=/usr/bin/vmq-admin")
package.input("_build/default/rel/vernemq/data=/var/lib/vernemq/")
package.input("_build/default/rel/vernemq/etc/=/etc/vernemq/")
package.input("_build/default/rel/vernemq/bin/=/usr/lib/vernemq/bin/")
package.input("_build/default/rel/vernemq/lib=/usr/lib/vernemq/")
package.input("_build/default/rel/vernemq/releases=/usr/lib/vernemq/")
package.input("_build/default/rel/vernemq/erts-10.2.3=/usr/lib/vernemq/")
package.input("_build/default/rel/vernemq/share/schema=/usr/lib/vernemq/share/")
package.input("_build/default/rel/vernemq/log/=/var/log/vernemq/")
package.input("_build/default/rel/vernemq/share/lua=/usr/share/vernemq/")

# Now, add our init-scripts, systemd services, and so on:
pleaserun = package.convert(FPM::Package::PleaseRun)
pleaserun.input ["/usr/bin/vernemq", "foreground"]

# Create two output packages!
output_packages = []
output_packages << pleaserun.convert(FPM::Package::RPM)
output_packages << pleaserun.convert(FPM::Package::Deb)

# and write them both.
begin
  output_packages.each do |output_package|
    output = output_package.to_s
    output_package.output(output)

    puts "successfully created #{output}"
  end
ensure
  # defer cleanup until the end
  output_packages.each {|p| p.cleanup}
end

