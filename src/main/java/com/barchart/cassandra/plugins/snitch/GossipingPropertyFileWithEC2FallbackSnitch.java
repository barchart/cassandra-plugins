package com.barchart.cassandra.plugins.snitch;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.Ec2MultiRegionSnitch;
import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;

/**
 * Attempts to use cassandra-rackdc.properties for snitch configuration if it is
 * available, otherwise falls back to EC2 multi region snitch. Useful for hybrid
 * cluster configurations.
 *
 * @author jeremy
 */
public class GossipingPropertyFileWithEC2FallbackSnitch implements IEndpointSnitch {

	IEndpointSnitch delegate;

	public GossipingPropertyFileWithEC2FallbackSnitch() throws ConfigurationException {
		try {
			delegate = new GossipingPropertyFileSnitch();
		} catch (final ConfigurationException e) {
			try {
				delegate = new Ec2MultiRegionSnitch();
			} catch (final IOException e1) {
				throw new ConfigurationException("Could not initialize EC2 snitch", e1);
			}
		}
	}

	@Override
	public String getRack(final InetAddress endpoint) {
		return delegate.getRack(endpoint);
	}

	@Override
	public String getDatacenter(final InetAddress endpoint) {
		return delegate.getDatacenter(endpoint);
	}

	@Override
	public List<InetAddress> getSortedListByProximity(final InetAddress address,
			final Collection<InetAddress> unsortedAddress) {
		return delegate.getSortedListByProximity(address, unsortedAddress);
	}

	@Override
	public void sortByProximity(final InetAddress address, final List<InetAddress> addresses) {
		delegate.sortByProximity(address, addresses);
	}

	@Override
	public int compareEndpoints(final InetAddress target, final InetAddress a1, final InetAddress a2) {
		return delegate.compareEndpoints(target, a1, a2);
	}

	@Override
	public void gossiperStarting() {
		delegate.gossiperStarting();
	}

	@Override
	public boolean isWorthMergingForRangeQuery(final List<InetAddress> merged, final List<InetAddress> l1,
			final List<InetAddress> l2) {
		return delegate.isWorthMergingForRangeQuery(merged, l1, l2);
	}

}
